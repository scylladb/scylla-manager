// Copyright (C) 2017 ScyllaDB

package repair

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/cespare/xxhash"
	"github.com/gocql/gocql"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/scylladb/go-set/strset"
	"github.com/scylladb/gocqlx"
	"github.com/scylladb/gocqlx/qb"
	log "github.com/scylladb/golog"
	"github.com/scylladb/mermaid"
	"github.com/scylladb/mermaid/cluster"
	"github.com/scylladb/mermaid/internal/inexlist"
	"github.com/scylladb/mermaid/internal/timeutc"
	"github.com/scylladb/mermaid/sched/runner"
	"github.com/scylladb/mermaid/schema"
	"github.com/scylladb/mermaid/scyllaclient"
	"github.com/scylladb/mermaid/uuid"
)

// Service orchestrates cluster repairs.
type Service struct {
	session      *gocql.Session
	config       Config
	cluster      cluster.ProviderFunc
	client       scyllaclient.ProviderFunc
	active       map[uuid.UUID]uuid.UUID // maps cluster ID to active run ID
	activeMu     sync.Mutex
	workerCtx    context.Context
	workerCancel context.CancelFunc
	wg           sync.WaitGroup
	logger       log.Logger
}

// NewService creates a new service instance.
func NewService(session *gocql.Session, c Config, cp cluster.ProviderFunc, sp scyllaclient.ProviderFunc, l log.Logger) (*Service, error) {
	if session == nil || session.Closed() {
		return nil, errors.New("invalid session")
	}

	if err := c.Validate(); err != nil {
		return nil, errors.Wrap(err, "invalid config")
	}

	if cp == nil {
		return nil, errors.New("invalid cluster provider")
	}

	if sp == nil {
		return nil, errors.New("invalid scylla provider")
	}

	ctx, cancel := context.WithCancel(context.Background())

	return &Service{
		session:      session,
		config:       c,
		cluster:      cp,
		client:       sp,
		active:       make(map[uuid.UUID]uuid.UUID),
		workerCtx:    ctx,
		workerCancel: cancel,
		logger:       l,
	}, nil
}

// Init shall be called when the service starts. It marks running and
// stopping runs as stopped.
func (s *Service) Init(ctx context.Context) error {
	// list tasks
	var tasks []*Run
	stmt, names := qb.Select(schema.RepairRun.Name).Distinct(schema.RepairRun.PartKey...).ToCql()
	if err := gocqlx.Query(s.session.Query(stmt).WithContext(ctx), names).SelectRelease(&tasks); err != nil {
		return err
	}

	// get status
	stmt, names = schema.RepairRun.Select("id", "status")
	g := gocqlx.Query(s.session.Query(stmt).WithContext(ctx), names)
	defer g.Release()

	// update status
	stmt, names = schema.RepairRun.Update("status", "cause")
	u := gocqlx.Query(s.session.Query(stmt).WithContext(ctx), names)
	defer u.Release()

	for _, r := range tasks {
		if err := g.BindStruct(r).Get(r); err != nil {
			return err
		}
		if r.Status == runner.StatusRunning || r.Status == runner.StatusStopping {
			r.Status = runner.StatusAborted
			r.Cause = "service killed"
			if err := u.BindStruct(r).Exec(); err != nil {
				return err
			}
		}
	}

	return nil
}

func (s *Service) tryLockCluster(run *Run) error {
	s.activeMu.Lock()
	defer s.activeMu.Unlock()

	owner := s.active[run.ClusterID]
	if owner != uuid.Nil {
		return errors.Errorf("cluster owned by another run: %s", owner)
	}

	s.active[run.ClusterID] = run.ID
	return nil
}

func (s *Service) unlockCluster(run *Run) error {
	s.activeMu.Lock()
	defer s.activeMu.Unlock()

	owner := s.active[run.ClusterID]
	if owner == uuid.Nil {
		return errors.Errorf("not locked")
	}
	if owner != run.ID {
		return errors.Errorf("cluster owned by another run: %s", owner)
	}

	delete(s.active, run.ClusterID)
	return nil
}

// GetTarget converts runner properties into repair Target.
func (s *Service) GetTarget(ctx context.Context, clusterID uuid.UUID, p runner.Properties) (Target, error) {
	tp := taskProperties{
		TokenRanges: PrimaryTokenRanges,
		FailFast:    false,
	}

	if err := json.Unmarshal(p, &tp); err != nil {
		return Target{}, mermaid.ErrValidate(errors.Wrapf(err, "unable to parse runner properties: %s", p), "")
	}

	t := Target{
		Host:        tp.Host,
		WithHosts:   tp.WithHosts,
		TokenRanges: tp.TokenRanges,
		FailFast:    tp.FailFast,
		Opts:        runner.OptsFromContext(ctx),
	}

	var (
		err   error
		dcMap map[string][]string
	)
	t.DC, dcMap, err = s.getDCs(ctx, clusterID, &tp)
	if err != nil {
		return t, err
	}

	hosts := t.WithHosts
	if t.Host != "" {
		hosts = append(hosts, t.Host)
	}
	if err := validateHostsBelongToCluster(dcMap, hosts...); err != nil {
		return Target{}, mermaid.ErrValidate(err, "")
	}

	t.Units, err = s.getUnits(ctx, clusterID, &tp)
	if err != nil {
		return t, err
	}

	return t, nil
}

// getUnits loads this clusters available repair units filtered through the
// supplied filter read from the properties. If no units are found or the filter
// contains any invalid filter a validation error is returned.
func (s *Service) getUnits(ctx context.Context, clusterID uuid.UUID, tp *taskProperties) ([]Unit, error) {
	if err := validateKeyspaceFilters(tp.Keyspace); err != nil {
		return nil, err
	}

	inclExcl, err := inexlist.ParseInExList(decorateKeyspaceFilters(tp.Keyspace))
	if err != nil {
		return nil, err
	}

	c, _ := s.client(ctx, clusterID)
	keyspaces, err := c.Keyspaces(ctx)
	if err != nil {
		return nil, errors.Wrapf(err, "unable to read keyspaces")
	}

	var units []Unit

	for _, keyspace := range keyspaces {
		tables, err := c.Tables(ctx, keyspace)
		if err != nil {
			return nil, errors.Wrapf(err, "unable to read table for keyspace %s", keyspace)
		}

		prefix := keyspace + "."
		for i := 0; i < len(tables); i++ {
			tables[i] = prefix + tables[i]
		}

		filteredTables := inclExcl.Filter(tables)
		for i := 0; i < len(filteredTables); i++ {
			filteredTables[i] = strings.TrimPrefix(filteredTables[i], prefix)
		}

		if len(filteredTables) == 0 {
			continue
		}

		unit := Unit{
			Keyspace:  keyspace,
			Tables:    filteredTables,
			allTables: len(filteredTables) == len(tables),
		}

		units = append(units, unit)
	}

	if len(units) == 0 {
		return nil, mermaid.ErrValidate(errors.Errorf("no matching units found for filters, ks=%s, dc=%s", tp.Keyspace, tp.DC), "")
	}

	sortUnits(units, inclExcl)

	return units, nil
}

// getDCs loads this clusters available datacenters filtered through the
// supplied filter read from the properties. If no DCs are found or the filter
// contains any invalid filter a validation error is returned.
func (s *Service) getDCs(ctx context.Context, clusterID uuid.UUID, tp *taskProperties) ([]string, map[string][]string, error) {
	dcInclExcl, err := inexlist.ParseInExList(decorateDCFilters(tp.DC))
	if err != nil {
		return nil, nil, err
	}

	c, _ := s.client(ctx, clusterID)
	dcMap, err := c.Datacenters(ctx)
	if err != nil {
		return nil, nil, errors.Wrap(err, "unable to read datacenters")
	}

	dcs := make([]string, 0, len(dcMap))
	for dc := range dcMap {
		dcs = append(dcs, dc)
	}

	filteredDCs := dcInclExcl.Filter(dcs)
	if len(filteredDCs) == 0 {
		return nil, nil, mermaid.ErrValidate(errors.Errorf("no matching dc found for dc=%s", tp.DC), "")
	}

	return filteredDCs, dcMap, nil
}

// Repair starts an asynchronous repair process.
func (s *Service) Repair(ctx context.Context, clusterID, taskID, runID uuid.UUID, t Target) error {
	s.logger.Debug(ctx, "Repair",
		"cluster_id", clusterID,
		"task_id", taskID,
		"run_id", runID,
		"target", t,
	)

	run := &Run{
		ClusterID:   clusterID,
		TaskID:      taskID,
		ID:          runID,
		Units:       t.Units,
		DC:          t.DC,
		Host:        t.Host,
		TokenRanges: t.TokenRanges,
		WithHosts:   t.WithHosts,
		Status:      runner.StatusRunning,
		StartTime:   timeutc.Now(),

		failFast: t.FailFast,
	}

	// fail updates a run and passes the error
	fail := func(err error) error {
		run.Status = runner.StatusError
		run.Cause = err.Error()
		run.EndTime = timeutc.Now()
		s.putRunLogError(ctx, run)
		return err
	}

	// get cluster name
	c, err := s.cluster(ctx, run.ClusterID)
	if err != nil {
		return fail(errors.Wrap(err, "invalid cluster"))
	}
	run.clusterName = c.String()

	// make sure no other repairs are being run on that cluster
	if err := s.tryLockCluster(run); err != nil {
		s.logger.Debug(ctx, "Lock error", "error", err)
		return fail(ErrActiveRepair)
	}
	defer func() {
		if run.Status != runner.StatusRunning {
			if err := s.unlockCluster(run); err != nil {
				s.logger.Error(ctx, "Unlock error", "error", err)
			}
		}
	}()

	s.logger.Info(ctx, "Initialising repair",
		"cluster_id", run.ClusterID,
		"task_id", run.TaskID,
		"run_id", run.ID,
		"target", t,
	)

	// get the cluster client
	client, err := s.client(ctx, run.ClusterID)
	if err != nil {
		return fail(errors.Wrap(err, "failed to get client proxy"))
	}

	// validate units
	for i, u := range run.Units {
		if err := s.validateUnit(ctx, u, client); err != nil {
			return fail(errors.Wrapf(err, "unit %d invalid", i))
		}
	}

	// get the cluster topology hash
	run.TopologyHash, err = s.topologyHash(ctx, client)
	if err != nil {
		return fail(errors.Wrap(err, "failed to get topology hash"))
	}

	if t.Opts.Continue {
		if err := s.decorateWithPrevRun(ctx, run); err != nil {
			return fail(err)
		}
		if run.PrevID != uuid.Nil {
			s.putRunLogError(ctx, run)
		}
	}

	// check the cluster partitioner
	p, err := client.Partitioner(ctx)
	if err != nil {
		return fail(errors.Wrap(err, "failed to get client partitioner name"))
	}
	if p != scyllaclient.Murmur3Partitioner {
		return fail(errors.Errorf("unsupported partitioner %q, the only supported partitioner is %q", p, scyllaclient.Murmur3Partitioner))
	}

	// register the run
	if err := s.putRun(ctx, run); err != nil {
		return fail(errors.Wrap(err, "failed to register the run"))
	}

	// Lock is used to make sure that the initialisation sequence is finished
	// before starting the repair go routine. Otherwise code optimisations
	// caused data races.
	lock := make(chan struct{})
	defer close(lock)

	// spawn async repair
	s.wg.Add(1)
	go func() {
		// wait for unlock
		<-lock

		var (
			parentCtx   = ctx
			ctx, cancel = context.WithCancel(log.CopyTraceID(s.workerCtx, ctx))
		)

		defer func() {
			cancel()
			if err := s.unlockCluster(run); err != nil {
				s.logger.Error(ctx, "Unlock error", "error", err)
			}
			s.wg.Done()
		}()

		go s.reportRepairProgress(ctx, run) // closed on cancel

		for unit := range run.Units {
			if err := s.repairUnit(ctx, run, unit, client); err != nil {
				// Use parentCtx context, so that when worker context is canceled
				// error is properly saved.
				switch errors.Cause(err) {
				case errAborted:
					run.Status = runner.StatusAborted
					run.Cause = "service stopped"
					run.EndTime = timeutc.Now()
					s.putRunLogError(parentCtx, run)
				case errStopped:
					run.Status = runner.StatusStopped
					run.EndTime = timeutc.Now()
					s.putRunLogError(parentCtx, run)
				default:
					// fail user parentCtx by default
					fail(err) // nolint
				}
				return
			}
		}

		run.Status = runner.StatusDone
		run.EndTime = timeutc.Now()
		s.putRunLogError(ctx, run)
	}()

	return nil
}

func (s *Service) validateUnit(ctx context.Context, u Unit, client *scyllaclient.Client) error {
	all, err := client.Tables(ctx, u.Keyspace)
	if err != nil {
		return errors.Wrap(err, "failed to get keyspace info")
	}
	if len(all) == 0 {
		return mermaid.ErrValidate(errors.Errorf("empty keyspace %s", u.Keyspace), "invalid unit")
	}
	if err := validateSubset(u.Tables, all); err != nil {
		return mermaid.ErrValidate(errors.Wrap(err, "keyspace %s missing tables"), "invalid unit")
	}

	return nil
}

// decorateWithPrevRun gets task previous run and if it can be continued
// sets PrevID on the given run.
func (s *Service) decorateWithPrevRun(ctx context.Context, run *Run) error {
	prev, err := s.GetLastStartedRun(ctx, run.ClusterID, run.TaskID)
	if err == mermaid.ErrNotFound {
		return nil
	}
	if err != nil {
		return errors.Wrap(err, "failed to get previous run")
	}

	// check if can continue from prev
	s.logger.Info(ctx, "Found previous run", "prev_id", prev.ID)
	switch {
	case prev.Status == runner.StatusDone:
		s.logger.Info(ctx, "Starting from scratch: previous run is done")
		return nil
	case timeutc.Since(prev.StartTime) > s.config.MaxRunAge:
		s.logger.Info(ctx, "Starting from scratch: previous run is too old")
		return nil
	case prev.TopologyHash != run.TopologyHash:
		s.logger.Info(ctx, "Starting from scratch: topology changed")
		return nil
	}

	// decorate run
	run.PrevID = prev.ID
	run.prevProg, err = s.getProgress(ctx, &Run{
		ClusterID: run.ClusterID,
		TaskID:    run.TaskID,
		ID:        prev.ID,
	})
	if err != nil {
		return errors.Wrap(err, "failed to get the last run progress")
	}
	run.Units = prev.Units
	run.DC = prev.DC
	run.WithHosts = prev.WithHosts
	run.TokenRanges = prev.TokenRanges

	return nil
}

func (s *Service) repairUnit(ctx context.Context, run *Run, unit int, client *scyllaclient.Client) error {
	u := run.Units[unit]

	// get the ring description
	ksDCs, ring, err := client.DescribeRing(ctx, u.Keyspace)
	if err != nil {
		return errors.Wrap(err, "failed to get the ring description")
	}

	dc, err := s.resolveDC(ctx, client, u, run, ksDCs)
	if err != nil {
		return err
	}
	s.logger.Info(ctx, "Repairing",
		"keyspace", u.Keyspace,
		"coordinator_dc", dc,
	)
	run.Units[unit].CoordinatorDC = dc // would be persisted by caller
	run.Units[unit].allDCs = strset.New(ksDCs...).IsEqual(strset.New(run.DC...))

	hostSegments, err := s.hostSegments(run, dc, ring)
	if err != nil {
		return errors.Wrap(err, "segmentation failed")
	}

	var prog []RunProgress
	if run.prevProg != nil {
		// extract unit progress
		n := len(run.prevProg)
		idx := sort.Search(n, func(i int) bool {
			return run.prevProg[i].Unit >= unit
		})
		end := sort.Search(n, func(i int) bool {
			return run.prevProg[i].Unit >= unit+1
		})
		for i := idx; i < end; i++ {
			prog = append(prog, *run.prevProg[i])
		}
		// fix run ID
		for i := range prog {
			prog[i].RunID = run.ID
		}

		// check if hosts did not change
		prevHosts := strset.New()
		for _, p := range prog {
			prevHosts.Add(p.Host)
		}
		hosts := strset.New()
		for host := range hostSegments {
			hosts.Add(host)
		}
		if diff := strset.SymmetricDifference(prevHosts, hosts); !diff.IsEmpty() {
			s.logger.Info(ctx, "Starting from scratch: hosts changed", "diff", diff)
			prog = nil
		}
	}
	if prog == nil {
		for host := range hostSegments {
			prog = append(prog, RunProgress{
				ClusterID: run.ClusterID,
				TaskID:    run.TaskID,
				RunID:     run.ID,
				Unit:      unit,
				Host:      host,
			})
		}
	}
	for _, p := range prog {
		if err := s.putRunProgress(ctx, &p); err != nil {
			return errors.Wrapf(err, "failed to initialise the run progress %v", &p)
		}
		// init metrics
		l := prometheus.Labels{
			"cluster":  run.clusterName,
			"task":     run.TaskID.String(),
			"keyspace": u.Keyspace,
			"host":     p.Host,
			"shard":    fmt.Sprint(p.Shard),
		}
		repairSegmentsTotal.With(l).Set(float64(p.SegmentCount))
		repairSegmentsSuccess.With(l).Set(float64(p.SegmentSuccess))
		repairSegmentsError.With(l).Set(float64(p.SegmentError))
	}

	// shuffle hosts
	hosts := make([]string, 0, len(hostSegments))
	for host := range hostSegments {
		hosts = append(hosts, host)
	}
	sort.Slice(hosts, func(i, j int) bool {
		return xxhash.Sum64String(hosts[i]) < xxhash.Sum64String(hosts[j])
	})

	// calculate number of tries
	allComplete := true
	for _, p := range prog {
		if !p.complete() || !p.completeWithErrors() {
			allComplete = false
		}
	}
	tries := 1
	if !allComplete {
		tries++
	}

	const pingTimeout = 5 * time.Second
	for ; tries > 0; tries-- {
		failed := false
		for _, host := range hosts {
			// ensure topology did not change
			th, err := s.topologyHash(ctx, client)
			if err != nil {
				s.logger.Info(ctx, "Topology check error", "error", err)
			} else if run.TopologyHash != th {
				return errors.Errorf("topology changed old hash: %s new hash: %s", run.TopologyHash, th)
			}

			// ping host
			if _, err := client.Ping(ctx, pingTimeout, host); err != nil {
				return errors.Wrapf(err, "host %s not available", host)
			}

			w := worker{
				Config:   &s.config,
				Run:      run,
				Unit:     unit,
				Host:     host,
				Segments: hostSegments[host],

				Service: s,
				Client:  client,
				Logger:  s.logger.Named("worker").With("host", host),
			}
			if err := w.exec(ctx); err != nil {
				if errors.Cause(err) == errDoneWithErrors && !run.failFast {
					failed = true
				} else {
					return err
				}
			}
		}
		if !failed {
			return nil
		}
	}

	return errDoneWithErrors
}

func (s *Service) resolveDC(ctx context.Context, client *scyllaclient.Client, u Unit, run *Run, ksDCs []string) (string, error) {
	var (
		dc  string
		err error
	)
	switch {
	case u.CoordinatorDC != "":
		dc = u.CoordinatorDC
	case run.Host != "":
		dc, err = client.HostDatacenter(ctx, run.Host)
		if err != nil {
			return "", errors.Wrap(err, "failed to resolve coordinator DC")
		}
	default:
		dc, err = s.getCoordinatorDC(ctx, client, run.DC, ksDCs)
		if err != nil {
			return "", errors.Wrap(err, "failed to resolve coordinator DC")
		}
	}
	return dc, nil
}

func (s *Service) getCoordinatorDC(ctx context.Context, client *scyllaclient.Client, runDCs []string, ksDCs []string) (string, error) {
	runSet := strset.Intersection(strset.New(runDCs...), strset.New(ksDCs...))
	if runSet.IsEmpty() {
		return "", errors.New("no matching DCs")
	}

	local, err := client.Datacenter(ctx)
	if err != nil {
		return "", errors.Wrapf(err, "failed to get local DC")
	}
	if runSet.Has(local) {
		return local, nil
	}

	all, err := client.Datacenters(ctx)
	if err != nil {
		return "", errors.Wrapf(err, "failed to get datacenters")
	}
	allSet := strset.New()
	for dc := range all {
		allSet.Add(dc)
	}

	dcHosts := make(map[string][]string)
	for dc, hosts := range all {
		if runSet.Has(dc) {
			dcHosts[dc] = hosts
		}
	}

	return client.ClosestDC(ctx, dcHosts)
}

func (s *Service) hostSegments(run *Run, dc string, ring []*scyllaclient.TokenRange) (map[string]segments, error) {
	// split token range into coordination hosts
	hs, err := groupSegmentsByHost(dc, run.WithHosts, run.TokenRanges, ring)
	if err != nil {
		return nil, err
	}
	// repair-with hosts are not coordinator hosts
	for _, host := range run.WithHosts {
		delete(hs, host)
	}
	// if we have a specific host other hosts are removed
	if run.Host != "" {
		if segs, ok := hs[run.Host]; ok {
			hs = map[string]segments{
				run.Host: segs,
			}
		} else {
			return nil, errors.Errorf("no segments available for the host %s", run.Host)
		}
	}

	return hs, nil
}

func (s *Service) reportRepairProgress(ctx context.Context, run *Run) {
	t := time.NewTicker(2500 * time.Millisecond)
	defer t.Stop()

	for {
		select {
		case <-t.C:
			s.reportRepairProgressMetric(ctx, run)
		case <-ctx.Done():
			// we need to update metrics one last time
			s.reportRepairProgressMetric(context.Background(), run)
			return
		}
	}
}

func (s *Service) reportRepairProgressMetric(ctx context.Context, run *Run) {
	prog, err := s.getProgress(ctx, run)
	if err != nil {
		s.logger.Error(ctx, "Failed to get hosts progress", "error", err)
	}

	p := aggregateProgress(run, prog)
	for _, u := range p.Units {
		for _, n := range u.Nodes {
			repairProgress.With(prometheus.Labels{
				"cluster":  run.clusterName,
				"task":     run.TaskID.String(),
				"keyspace": u.Unit.Keyspace,
				"host":     n.Host,
			}).Set(float64(n.PercentComplete))
		}
	}
}

func (s *Service) topologyHash(ctx context.Context, client *scyllaclient.Client) (uuid.UUID, error) {
	tokens, err := client.Tokens(ctx)
	if err != nil {
		return uuid.Nil, errors.Wrap(err, "failed to get the cluster tokens")
	}

	return topologyHash(tokens), nil
}

// GetLastStartedRun returns the the most recent successful run of the task.
func (s *Service) GetLastStartedRun(ctx context.Context, clusterID, taskID uuid.UUID) (*Run, error) {
	s.logger.Debug(ctx, "GetLastStartedRun",
		"cluster_id", clusterID,
		"task_id", taskID,
	)

	stmt, names := qb.Select(schema.RepairRun.Name).Where(
		qb.Eq("cluster_id"), qb.Eq("task_id"),
	).Limit(100).ToCql()

	q := gocqlx.Query(s.session.Query(stmt).WithContext(ctx), names).BindMap(qb.M{
		"cluster_id": clusterID,
		"task_id":    taskID,
	})
	defer q.Release()

	if q.Err() != nil {
		return nil, q.Err()
	}

	var runs []*Run
	if err := gocqlx.Select(&runs, q.Query); err != nil {
		return nil, err
	}

	for _, r := range runs {
		if r.Status != runner.StatusError {
			return r, nil
		}

		// check if repair started
		p, err := s.getProgress(ctx, r)
		if err != nil {
			return nil, err
		}
		if len(p) > 0 {
			return r, nil
		}
	}

	return nil, mermaid.ErrNotFound
}

// GetRun returns a run based on ID. If nothing was found mermaid.ErrNotFound
// is returned.
func (s *Service) GetRun(ctx context.Context, clusterID, taskID, runID uuid.UUID) (*Run, error) {
	s.logger.Debug(ctx, "GetRun",
		"cluster_id", clusterID,
		"task_id", taskID,
		"run_id", runID,
	)

	stmt, names := schema.RepairRun.Get()

	q := gocqlx.Query(s.session.Query(stmt).WithContext(ctx), names).BindMap(qb.M{
		"cluster_id": clusterID,
		"task_id":    taskID,
		"id":         runID,
	})

	var r Run
	return &r, q.GetRelease(&r)
}

// putRun upserts a repair run.
func (s *Service) putRun(ctx context.Context, r *Run) error {
	stmt, names := schema.RepairRun.Insert()
	q := gocqlx.Query(s.session.Query(stmt).WithContext(ctx), names).BindStruct(r)
	return q.ExecRelease()
}

// putRunLogError executes putRun and consumes the error.
func (s *Service) putRunLogError(ctx context.Context, r *Run) {
	if err := s.putRun(ctx, r); err != nil {
		s.logger.Error(ctx, "Cannot update the run",
			"run", &r,
			"error", err,
		)
	}
}

// StopRepair marks a running repair as stopping.
func (s *Service) StopRepair(ctx context.Context, clusterID, taskID, runID uuid.UUID) error {
	s.logger.Debug(ctx, "StopRepair",
		"cluster_id", clusterID,
		"task_id", taskID,
		"run_id", runID,
	)

	r, err := s.GetRun(ctx, clusterID, taskID, runID)
	if err != nil {
		return err
	}

	if r.Status != runner.StatusRunning {
		return errors.New("not running")
	}

	s.logger.Info(ctx, "Stopping repair",
		"cluster_id", clusterID,
		"task_id", taskID,
		"run_id", runID,
	)

	r.Status = runner.StatusStopping

	stmt, names := schema.RepairRun.Update("status")
	q := gocqlx.Query(s.session.Query(stmt).WithContext(ctx), names).BindStruct(r)

	return q.ExecRelease()
}

// isStopped checks if repair is in StatusStopping or StatusStopped.
func (s *Service) isStopped(ctx context.Context, run *Run) (bool, error) {
	stmt, names := schema.RepairRun.Get("status")
	q := gocqlx.Query(s.session.Query(stmt).WithContext(ctx), names).BindStruct(run)
	if q.Err() != nil {
		return false, q.Err()
	}

	var v runner.Status
	if err := q.Query.Scan(&v); err != nil {
		return false, err
	}

	return v == runner.StatusStopping || v == runner.StatusStopped, nil
}

// GetProgress returns run progress for all shards on all the hosts. If nothing
// was found mermaid.ErrNotFound is returned.
func (s *Service) GetProgress(ctx context.Context, clusterID, taskID, runID uuid.UUID) (Progress, error) {
	s.logger.Debug(ctx, "GetProgress",
		"cluster_id", clusterID,
		"task_id", taskID,
		"run_id", runID,
	)

	run, err := s.GetRun(ctx, clusterID, taskID, runID)
	if err != nil {
		return Progress{}, err
	}

	prog, err := s.getProgress(ctx, run)
	if err != nil {
		return Progress{}, err
	}

	return aggregateProgress(run, prog), nil
}

func (s *Service) getProgress(ctx context.Context, run *Run) ([]*RunProgress, error) {
	stmt, names := schema.RepairRunProgress.Select()

	q := gocqlx.Query(s.session.Query(stmt).WithContext(ctx), names).BindMap(qb.M{
		"cluster_id": run.ClusterID,
		"task_id":    run.TaskID,
		"run_id":     run.ID,
	})

	var p []*RunProgress
	return p, q.SelectRelease(&p)
}

func (s *Service) getHostProgress(ctx context.Context, run *Run, unit int, host string) ([]*RunProgress, error) {
	stmt, names := schema.RepairRunProgress.SelectBuilder().Where(
		qb.Eq("unit"), qb.Eq("host"),
	).ToCql()

	q := gocqlx.Query(s.session.Query(stmt).WithContext(ctx), names).BindMap(qb.M{
		"cluster_id": run.ClusterID,
		"task_id":    run.TaskID,
		"run_id":     run.ID,
		"unit":       unit,
		"host":       host,
	})

	var p []*RunProgress
	return p, q.SelectRelease(&p)
}

// putRunProgress upserts a repair run.
func (s *Service) putRunProgress(ctx context.Context, p *RunProgress) error {
	s.logger.Debug(ctx, "PutRunProgress", "run_progress", p)

	stmt, names := schema.RepairRunProgress.Insert()
	q := gocqlx.Query(s.session.Query(stmt).WithContext(ctx), names).BindStruct(p)

	return q.ExecRelease()
}

// Close terminates all the worker routines.
func (s *Service) Close() {
	s.workerCancel()
	s.wg.Wait()
}
