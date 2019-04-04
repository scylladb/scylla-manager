// Copyright (C) 2017 ScyllaDB

package repair

import (
	"context"
	"encoding/json"
	"sort"
	"strings"
	"sync"

	"github.com/cespare/xxhash"
	"github.com/gocql/gocql"
	"github.com/pkg/errors"
	"github.com/scylladb/go-log"
	"github.com/scylladb/go-set/strset"
	"github.com/scylladb/gocqlx"
	"github.com/scylladb/gocqlx/qb"
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
	activeRun    map[uuid.UUID]uuid.UUID // maps cluster ID to active run ID
	activeRunMu  sync.Mutex
	runCancel    map[uuid.UUID]context.CancelFunc
	runCancelMu  sync.Mutex
	workerCtx    context.Context
	workerCancel context.CancelFunc
	wg           sync.WaitGroup
	logger       log.Logger
}

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
		activeRun:    make(map[uuid.UUID]uuid.UUID),
		runCancel:    make(map[uuid.UUID]context.CancelFunc),
		workerCtx:    ctx,
		workerCancel: cancel,
		logger:       l,
	}, nil
}

// Runner creates an runner.Runner instance that handles repairs.
func (s *Service) Runner() runner.Runner {
	return repairRunner{service: s}
}

func (s *Service) tryLockCluster(clusterID, runID uuid.UUID) error {
	s.activeRunMu.Lock()
	defer s.activeRunMu.Unlock()

	owner := s.activeRun[clusterID]
	if owner != uuid.Nil {
		return errors.Errorf("cluster owned by another run: %s", owner)
	}

	s.activeRun[clusterID] = runID
	return nil
}

func (s *Service) unlockCluster(clusterID, runID uuid.UUID) error {
	s.activeRunMu.Lock()
	defer s.activeRunMu.Unlock()

	owner := s.activeRun[clusterID]
	if owner == uuid.Nil {
		return errors.Errorf("not locked")
	}
	if owner != runID {
		return errors.Errorf("cluster owned by another run: %s", owner)
	}

	delete(s.activeRun, clusterID)
	return nil
}

func (s *Service) newRunContext(ctx context.Context, run *Run) (context.Context, context.CancelFunc) {
	ctx, cancel := context.WithCancel(log.CopyTraceID(s.workerCtx, ctx))

	deleteAndCancel := func() {
		s.runCancelMu.Lock()
		delete(s.runCancel, run.ID)
		s.runCancelMu.Unlock()
		cancel()
	}

	s.runCancelMu.Lock()
	s.runCancel[run.ID] = deleteAndCancel
	s.runCancelMu.Unlock()

	return ctx, deleteAndCancel
}

func (s *Service) cancelRunContext(runID uuid.UUID) {
	s.runCancelMu.Lock()
	cancel, ok := s.runCancel[runID]
	s.runCancelMu.Unlock()
	if ok {
		cancel()
	}
}

// GetTarget converts runner properties into repair Target.
func (s *Service) GetTarget(ctx context.Context, clusterID uuid.UUID, p runner.Properties, force bool) (Target, error) {
	tp := taskProperties{
		TokenRanges: DCPrimaryTokenRanges,
		FailFast:    false,
	}

	if err := json.Unmarshal(p, &tp); err != nil {
		return Target{}, mermaid.ErrValidate(errors.Wrapf(err, "failed to parse runner properties: %s", p), "")
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
	t.DC, dcMap, err = s.getDCs(ctx, clusterID, tp.DC)
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

	t.Units, err = s.getUnits(ctx, clusterID, tp.Keyspace)
	if err != nil {
		return t, err
	}

	if len(t.Units) == 0 && !force {
		return t, mermaid.ErrValidate(errors.Errorf("no matching units found for filters, ks=%s", tp.Keyspace), "")
	}

	return t, nil
}

// getDCs loads available datacenters filtered through the supplied filter.
// If no DCs are found or a filter is invalid a validation error is returned.
func (s *Service) getDCs(ctx context.Context, clusterID uuid.UUID, filters []string) ([]string, map[string][]string, error) {
	dcInclExcl, err := inexlist.ParseInExList(decorateDCFilters(filters))
	if err != nil {
		return nil, nil, err
	}

	c, err := s.client(ctx, clusterID)
	if err != nil {
		return nil, nil, errors.Wrapf(err, "failed to get client")
	}
	dcMap, err := c.Datacenters(ctx)
	if err != nil {
		return nil, nil, errors.Wrap(err, "failed to read datacenters")
	}

	dcs := make([]string, 0, len(dcMap))
	for dc := range dcMap {
		dcs = append(dcs, dc)
	}

	filteredDCs := dcInclExcl.Filter(dcs)
	if len(filteredDCs) == 0 {
		return nil, nil, mermaid.ErrValidate(errors.Errorf("no matching dc found for dc=%s", filters), "")
	}
	sort.Strings(filteredDCs)

	return filteredDCs, dcMap, nil
}

// getUnits loads available repair units (keyspaces) filtered through the
// supplied filters. If no units are found or a filter is invalid a validation
// error is returned.
func (s *Service) getUnits(ctx context.Context, clusterID uuid.UUID, filters []string) ([]Unit, error) {
	if err := validateKeyspaceFilters(filters); err != nil {
		return nil, err
	}

	inclExcl, err := inexlist.ParseInExList(decorateKeyspaceFilters(filters))
	if err != nil {
		return nil, err
	}

	client, err := s.client(ctx, clusterID)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to get client")
	}
	keyspaces, err := client.Keyspaces(ctx)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to read keyspaces")
	}

	var units []Unit

	for _, keyspace := range keyspaces {
		tables, err := client.Tables(ctx, keyspace)
		if err != nil {
			return nil, errors.Wrapf(err, "keyspace %s: failed to get tables", keyspace)
		}

		prefix := keyspace + "."
		for i := 0; i < len(tables); i++ {
			tables[i] = prefix + tables[i]
		}

		filteredTables := inclExcl.Filter(tables)

		// no data, skip the keyspace
		if len(filteredTables) == 0 {
			continue
		}

		for i := 0; i < len(filteredTables); i++ {
			filteredTables[i] = strings.TrimPrefix(filteredTables[i], prefix)
		}

		// get the ring description
		ring, err := client.DescribeRing(ctx, keyspace)
		if err != nil {
			return nil, errors.Wrapf(err, "keyspace %s: failed to get ring description", keyspace)
		}

		// local data, skip the keyspace
		if ring.Replication == scyllaclient.LocalStrategy {
			continue
		}

		u := Unit{
			Keyspace:  keyspace,
			Tables:    filteredTables,
			AllTables: len(filteredTables) == len(tables),
		}
		units = append(units, u)
	}

	sortUnits(units, inclExcl)

	return units, nil
}

// Repair starts an asynchronous repair process.
func (s *Service) Repair(ctx context.Context, clusterID, taskID, runID uuid.UUID, t Target) error {
	s.logger.Debug(ctx, "Repair",
		"cluster_id", clusterID,
		"task_id", taskID,
		"run_id", runID,
		"target", t,
	)

	// Lock is used to make sure that the initialization sequence is finished
	// before starting the repair go routine. Otherwise code optimizations
	// caused data races.
	lock := make(chan struct{})

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
	if err := s.tryLockCluster(run.ClusterID, run.ID); err != nil {
		s.logger.Debug(ctx, "Lock error", "error", err)
		return fail(ErrActiveRepair)
	}
	defer func() {
		if run.Status != runner.StatusRunning {
			if err := s.unlockCluster(run.ClusterID, run.ID); err != nil {
				s.logger.Error(ctx, "Unlock error", "error", err)
			}
		}
		// "lock" protects "run" so it needs to be closed after finishing usage of "run".
		close(lock)
	}()

	s.logger.Info(ctx, "Initializing repair",
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
	if err := s.putRun(run); err != nil {
		return fail(errors.Wrap(err, "failed to register the run"))
	}

	// spawn async repair
	s.wg.Add(1)
	go func() {
		// wait for unlock
		<-lock
		s.repair(ctx, run, client)
	}()

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
	case timeutc.Since(prev.StartTime) > s.config.AgeMax:
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

func (s *Service) repair(ctx context.Context, run *Run, client *scyllaclient.Client) {
	// Use parent context, so that when worker context is canceled
	// error is properly saved.
	runCtx, cancel := s.newRunContext(ctx, run)

	defer func() {
		cancel()
		if err := s.unlockCluster(run.ClusterID, run.ID); err != nil {
			s.logger.Error(ctx, "Unlock error", "error", err)
		}
		s.wg.Done()
	}()

	run.unitWorkers = make([]unitWorker, len(run.Units))
	for unit := range run.Units {
		if err := s.initUnitWorker(runCtx, run, unit, client); err != nil {
			run.Status = runner.StatusError
			run.Cause = errors.Wrapf(err, "failed to prepare repair for keyspace %s", run.Units[unit].Keyspace).Error()
			run.EndTime = timeutc.Now()
			s.putRunLogError(ctx, run)
			return
		}
	}

	for unit := range run.Units {
		if err := s.repairUnit(runCtx, run, unit, client); err != nil {
			if errors.Cause(err) == errStopped {
				run.Status = runner.StatusStopped
				// service is stopping promote stop to abort
				if s.workerCtx.Err() != nil {
					run.Status = runner.StatusAborted
					run.Cause = "service stopped"
				}
			} else {
				run.Status = runner.StatusError
				run.Cause = errors.Wrapf(err, "keyspace %s", run.Units[unit].Keyspace).Error()
			}
			run.EndTime = timeutc.Now()
			s.putRunLogError(ctx, run)
			return
		}
	}

	run.Status = runner.StatusDone
	run.EndTime = timeutc.Now()
	s.putRunLogError(ctx, run)
}

func (s *Service) initUnitWorker(ctx context.Context, run *Run, unit int, client *scyllaclient.Client) error {
	u := &run.Units[unit] // pointer to modify the unit

	// get the ring description if needed
	ring, err := client.DescribeRing(ctx, u.Keyspace)
	if err != nil {
		return errors.Wrap(err, "failed to get the ring description")
	}
	ksDCs := ring.Datacenters()

	u.hosts = s.repairedHosts(run, ring)
	u.allDCs = strset.New(ksDCs...).IsEqual(strset.New(run.DC...))

	// get hosts and segments to repair
	var hostSegments map[string]segments

	if ring.Replication == scyllaclient.SimpleStrategy && len(ksDCs) > 1 {
		s.logger.Info(ctx, "SimpleStrategy replication strategy detected, conciser changing to NetworkTopologyStrategy",
			"keyspace", u.Keyspace,
		)

		// downgrade DCPrimaryTokenRanges to PrimaryTokenRanges since there is
		// no coordinator DC
		tr := run.TokenRanges
		if tr == DCPrimaryTokenRanges {
			tr = PrimaryTokenRanges
		}
		hostSegments = groupSegmentsByHost("", run.Host, run.WithHosts, tr, ring)

		if !u.allDCs {
			s.logger.Info(ctx, "SimpleStrategy replication strategy with DC filtering may result in not all tokens being repaired in selected DCs ignoring DC filtering",
				"keyspace", u.Keyspace,
			)
		}
	} else {
		// get coordinator DC
		dc, err := s.resolveDC(ctx, client, *u, run, ksDCs)
		if err != nil {
			return err
		}
		u.CoordinatorDC = dc

		hostSegments = groupSegmentsByHost(dc, run.Host, run.WithHosts, run.TokenRanges, ring)
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
	for i := range prog {
		if err := s.putRunProgress(ctx, &prog[i]); err != nil {
			return errors.Wrapf(err, "failed to initialize the run progress %v", prog[i])
		}
	}

	// shuffle hosts
	hosts := make([]string, 0, len(hostSegments))
	for host := range hostSegments {
		hosts = append(hosts, host)
	}
	sort.Slice(hosts, func(i, j int) bool {
		return xxhash.Sum64String(hosts[i]) < xxhash.Sum64String(hosts[j])
	})

	// prepare host workers for unit
	unitWorker := make([]*hostWorker, len(hosts))
	for i, host := range hosts {
		// ping host
		if _, err := client.Ping(ctx, host); err != nil {
			return errors.Wrapf(err, "host %s not available", host)
		}
		unitWorker[i] = &hostWorker{
			Config:   &s.config,
			Run:      run,
			Unit:     unit,
			Host:     host,
			Segments: hostSegments[host],

			Service: s,
			Client:  client,
			Logger:  s.logger.Named("worker").With("host", host),
		}
		if err := unitWorker[i].init(ctx); err != nil {
			return errors.Wrapf(err, "host %s: failed to init repair", host)
		}
	}
	run.unitWorkers[unit] = unitWorker

	return nil
}

func (s *Service) repairedHosts(run *Run, ring scyllaclient.Ring) (hosts []string) {
	dcFilter := strset.New(run.DC...)
	for h, dc := range ring.HostDC {
		if dcFilter.Has(dc) {
			hosts = append(hosts, h)
		}
	}
	return
}

func (s *Service) repairUnit(ctx context.Context, run *Run, unit int, client *scyllaclient.Client) error {
	s.logger.Info(ctx, "Repairing unit", "unit", run.Units[unit])

	// calculate number of tries
	allComplete := true
	for _, worker := range run.unitWorkers[unit] {
		for _, shard := range worker.shards {
			if !shard.progress.complete() || !shard.progress.completeWithErrors() {
				allComplete = false
			}
		}
	}
	tries := 1
	if !allComplete {
		tries++
	}

	for ; tries > 0; tries-- {
		failed := false
		for _, worker := range run.unitWorkers[unit] {
			// ensure topology did not change
			th, err := s.topologyHash(ctx, client)
			if err != nil {
				s.logger.Info(ctx, "Topology check error", "error", err)
			} else if run.TopologyHash != th {
				return errors.Errorf("topology changed old hash: %s new hash: %s", run.TopologyHash, th)
			}
			// run repair on host
			if err := worker.exec(ctx); err != nil {
				if errors.Cause(err) == errDoneWithErrors && !run.failFast {
					failed = true
				} else {
					return errors.Wrapf(err, "host %s", worker.Host)
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

	all, err := client.Datacenters(ctx)
	if err != nil {
		return "", errors.Wrapf(err, "failed to get datacenters")
	}

	dcHosts := make(map[string][]string)
	for dc, hosts := range all {
		if runSet.Has(dc) {
			dcHosts[dc] = hosts
		}
	}
	closest, err := client.ClosestDC(ctx, dcHosts)
	if err != nil {
		return "", nil
	}

	return closest[0], nil
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

	stmt, names := qb.Select(schema.RepairRun.Name()).Where(
		qb.Eq("cluster_id"), qb.Eq("task_id"),
	).Limit(100).ToCql()

	q := gocqlx.Query(s.session.Query(stmt), names).BindMap(qb.M{
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

	q := gocqlx.Query(s.session.Query(stmt), names).BindMap(qb.M{
		"cluster_id": clusterID,
		"task_id":    taskID,
		"id":         runID,
	})

	var r Run
	return &r, q.GetRelease(&r)
}

// putRun upserts a repair run.
func (s *Service) putRun(r *Run) error {
	stmt, names := schema.RepairRun.Insert()
	q := gocqlx.Query(s.session.Query(stmt), names).BindStruct(r)
	return q.ExecRelease()
}

// putRunLogError executes putRun and consumes the error.
func (s *Service) putRunLogError(ctx context.Context, r *Run) {
	if err := s.putRun(r); err != nil {
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
	defer s.cancelRunContext(runID)

	r.Status = runner.StatusStopping

	stmt, names := schema.RepairRun.Update("status")
	q := gocqlx.Query(s.session.Query(stmt), names).BindStruct(r)

	return q.ExecRelease()
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

	q := gocqlx.Query(s.session.Query(stmt), names).BindMap(qb.M{
		"cluster_id": run.ClusterID,
		"task_id":    run.TaskID,
		"run_id":     run.ID,
	})

	var p []*RunProgress
	return p, q.SelectRelease(&p)
}

func (s *Service) getHostProgress(run *Run, unit int, host string) ([]*RunProgress, error) {
	stmt, names := schema.RepairRunProgress.SelectBuilder().Where(
		qb.Eq("unit"), qb.Eq("host"),
	).ToCql()

	q := gocqlx.Query(s.session.Query(stmt), names).BindMap(qb.M{
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
	q := gocqlx.Query(s.session.Query(stmt), names).BindStruct(p)

	return q.ExecRelease()
}

// Close terminates all the worker routines.
func (s *Service) Close() {
	s.workerCancel()
	s.wg.Wait()
}
