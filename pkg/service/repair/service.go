// Copyright (C) 2017 ScyllaDB

package repair

import (
	"context"
	"encoding/json"
	"sort"

	"github.com/cespare/xxhash"
	"github.com/gocql/gocql"
	"github.com/pkg/errors"
	"github.com/scylladb/go-log"
	"github.com/scylladb/go-set/strset"
	"github.com/scylladb/gocqlx"
	"github.com/scylladb/gocqlx/qb"
	"github.com/scylladb/mermaid/pkg/schema/table"
	"github.com/scylladb/mermaid/pkg/scyllaclient"
	"github.com/scylladb/mermaid/pkg/service"
	"github.com/scylladb/mermaid/pkg/util/inexlist/dcfilter"
	"github.com/scylladb/mermaid/pkg/util/inexlist/ksfilter"
	"github.com/scylladb/mermaid/pkg/util/timeutc"
	"github.com/scylladb/mermaid/pkg/util/uuid"
	"go.uber.org/multierr"
)

// ClusterNameFunc returns name for a given ID.
type ClusterNameFunc func(ctx context.Context, clusterID uuid.UUID) (string, error)

// Service orchestrates clusterName repairs.
type Service struct {
	session *gocql.Session
	config  Config

	clusterName  ClusterNameFunc
	scyllaClient scyllaclient.ProviderFunc
	logger       log.Logger
}

func NewService(session *gocql.Session, config Config, clusterName ClusterNameFunc, scyllaClient scyllaclient.ProviderFunc, logger log.Logger) (*Service, error) {
	if session == nil || session.Closed() {
		return nil, errors.New("invalid session")
	}

	if err := config.Validate(); err != nil {
		return nil, errors.Wrap(err, "invalid config")
	}

	if clusterName == nil {
		return nil, errors.New("invalid cluster name provider")
	}

	if scyllaClient == nil {
		return nil, errors.New("invalid scylla provider")
	}

	return &Service{
		session:      session,
		config:       config,
		clusterName:  clusterName,
		scyllaClient: scyllaClient,
		logger:       logger,
	}, nil
}

// Runner creates a Runner that handles repairs.
func (s *Service) Runner() Runner {
	return Runner{service: s}
}

// GetTarget converts runner properties into repair Target.
func (s *Service) GetTarget(ctx context.Context, clusterID uuid.UUID, properties json.RawMessage, force bool) (Target, error) {
	p := taskProperties{
		TokenRanges: DCPrimaryTokenRanges,
		Continue:    true,
	}

	if err := json.Unmarshal(properties, &p); err != nil {
		return Target{}, service.ErrValidate(errors.Wrapf(err, "parse runner properties: %s", properties))
	}

	t := Target{
		Host:        p.Host,
		WithHosts:   p.WithHosts,
		TokenRanges: p.TokenRanges,
		FailFast:    p.FailFast,
		Continue:    p.Continue,
	}

	client, err := s.scyllaClient(ctx, clusterID)
	if err != nil {
		return t, errors.Wrapf(err, "get client")
	}

	// Get hosts in DCs
	dcMap, err := client.Datacenters(ctx)
	if err != nil {
		return t, errors.Wrap(err, "read datacenters")
	}

	hosts := t.WithHosts
	if t.Host != "" {
		hosts = append(hosts, t.Host)
	}
	if err := validateHostsBelongToCluster(dcMap, hosts...); err != nil {
		return t, service.ErrValidate(err)
	}

	// Filter DCs
	if t.DC, err = dcfilter.Apply(dcMap, p.DC); err != nil {
		return t, err
	}

	// Filter keyspaces
	f, err := ksfilter.NewFilter(p.Keyspace)
	if err != nil {
		return t, err
	}

	keyspaces, err := client.Keyspaces(ctx)
	if err != nil {
		return t, errors.Wrapf(err, "read keyspaces")
	}
	for _, keyspace := range keyspaces {
		tables, err := client.Tables(ctx, keyspace)
		if err != nil {
			return t, errors.Wrapf(err, "keyspace %s: get tables", keyspace)
		}

		// Get the ring description and skip local data
		ring, err := client.DescribeRing(ctx, keyspace)
		if err != nil {
			return t, errors.Wrapf(err, "keyspace %s: get ring description", keyspace)
		}
		if ring.Replication == scyllaclient.LocalStrategy {
			continue
		}

		// Add to the filter
		f.Add(keyspace, tables)
	}

	// Get the filtered units
	v, err := f.Apply(force)
	if err != nil {
		return t, err
	}

	// Copy units
	for _, u := range v {
		t.Units = append(t.Units, Unit{
			Keyspace:  u.Keyspace,
			Tables:    u.Tables,
			AllTables: u.AllTables,
		})
	}

	return t, nil
}

// Repair performs the repair process on the Target.
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
		StartTime:   timeutc.Now(),

		failFast: t.FailFast,
	}

	// Get cluster name
	clusterName, err := s.clusterName(ctx, run.ClusterID)
	if err != nil {
		return errors.Wrap(err, "invalid cluster")
	}
	run.clusterName = clusterName

	s.logger.Info(ctx, "Initializing repair",
		"cluster_id", run.ClusterID,
		"task_id", run.TaskID,
		"run_id", run.ID,
		"target", t,
	)

	// Get the cluster client
	client, err := s.scyllaClient(ctx, run.ClusterID)
	if err != nil {
		return errors.Wrap(err, "get client proxy")
	}

	// Get the cluster topology hash
	run.TopologyHash, err = s.topologyHash(ctx, client)
	if err != nil {
		return errors.Wrap(err, "get topology hash")
	}

	if t.Continue {
		if err := s.decorateWithPrevRun(ctx, run); err != nil {
			return err
		}
		if run.PrevID != uuid.Nil {
			s.putRunLogError(ctx, run)
		}
	}

	// Check the cluster partitioner
	p, err := client.Partitioner(ctx)
	if err != nil {
		return errors.Wrap(err, "get client partitioner name")
	}
	if p != scyllaclient.Murmur3Partitioner {
		return errors.Errorf("unsupported partitioner %q, the only supported partitioner is %q", p, scyllaclient.Murmur3Partitioner)
	}

	// Register the run
	if err := s.putRun(run); err != nil {
		return errors.Wrap(err, "register the run")
	}

	// Initialize unit runners
	run.unitWorkers = make([]unitWorker, len(run.Units))
	for unit := range run.Units {
		if err := s.initUnitWorker(ctx, run, unit, client); err != nil {
			return errors.Wrapf(err, "prepare repair for keyspace %s", run.Units[unit].Keyspace)
		}
	}
	s.putRunLogError(ctx, run)

	// Repair units
	for unit := range run.Units {
		if err := s.repairUnit(ctx, run, unit, client); err != nil {
			return errors.Wrapf(err, "keyspace %s", run.Units[unit].Keyspace)
		}
	}

	return nil
}

// decorateWithPrevRun gets task previous run and if it can be continued
// sets PrevID on the given run.
func (s *Service) decorateWithPrevRun(ctx context.Context, run *Run) error {
	prev, err := s.GetLastResumableRun(ctx, run.ClusterID, run.TaskID)
	if err == service.ErrNotFound {
		return nil
	}
	if err != nil {
		return errors.Wrap(err, "get previous run")
	}

	// Check if can continue from prev
	s.logger.Info(ctx, "Found previous run", "prev_id", prev.ID)
	switch {
	case timeutc.Since(prev.StartTime) > s.config.AgeMax:
		s.logger.Info(ctx, "Starting from scratch: previous run is too old")
		return nil
	case prev.TopologyHash != run.TopologyHash:
		s.logger.Info(ctx, "Starting from scratch: topology changed")
		return nil
	}

	// Decorate run
	run.PrevID = prev.ID
	run.prevProg, err = s.getProgress(ctx, &Run{
		ClusterID: run.ClusterID,
		TaskID:    run.TaskID,
		ID:        prev.ID,
	})
	if err != nil {
		return errors.Wrap(err, "get the last run progress")
	}
	run.Units = prev.Units
	run.DC = prev.DC
	run.WithHosts = prev.WithHosts
	run.TokenRanges = prev.TokenRanges

	return nil
}

func (s *Service) initUnitWorker(ctx context.Context, run *Run, unit int, client *scyllaclient.Client) error {
	u := &run.Units[unit] // pointer to modify the unit

	// Get the ring description if needed
	ring, err := client.DescribeRing(ctx, u.Keyspace)
	if err != nil {
		return errors.Wrap(err, "get the ring description")
	}
	ksDCs := ring.Datacenters()

	u.hosts = s.repairedHosts(run, ring)
	u.allDCs = strset.New(ksDCs...).IsEqual(strset.New(run.DC...))

	// Get hosts and segments to repair
	var hostSegments map[string]segments

	if ring.Replication == scyllaclient.SimpleStrategy && len(ksDCs) > 1 {
		s.logger.Info(ctx, "SimpleStrategy replication strategy detected, conciser changing to NetworkTopologyStrategy",
			"keyspace", u.Keyspace,
		)

		// Downgrade DCPrimaryTokenRanges to PrimaryTokenRanges since there is
		// no coordinator DC
		tr := run.TokenRanges
		if tr == DCPrimaryTokenRanges {
			tr = PrimaryTokenRanges
		}
		hostSegments = groupSegmentsByHost("", run.Host, run.WithHosts, tr, ring)

		if !u.allDCs {
			s.logger.Info(ctx, "SimpleStrategy replication strategy with DC filtering may result in not all tokens being repaired in selected DCs ignoring DC filtering",
				"cluster_id", run.ClusterID,
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
			return run.prevProg[i].Unit > unit
		})
		for i := idx; i < end; i++ {
			prog = append(prog, *run.prevProg[i])
		}
		// fix run ID
		for i := range prog {
			prog[i].RunID = run.ID
		}

		// Check if hosts did not change
		prevHosts := strset.New()
		for i := range prog {
			prevHosts.Add(prog[i].Host)
		}
		hosts := strset.New()
		for host := range hostSegments {
			hosts.Add(host)
		}
		if diff := strset.SymmetricDifference(prevHosts, hosts); !diff.IsEmpty() {
			s.logger.Info(ctx, "Starting from scratch: hosts changed",
				"keyspace", u.Keyspace,
				"cur", hosts.List(),
				"prev", prevHosts.List(),
			)
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
			return errors.Wrapf(err, "initialize the run progress %v", prog[i])
		}
	}

	// Shuffle hosts
	hosts := make([]string, 0, len(hostSegments))
	for host := range hostSegments {
		hosts = append(hosts, host)
	}
	sort.Slice(hosts, func(i, j int) bool {
		return xxhash.Sum64String(hosts[i]) < xxhash.Sum64String(hosts[j])
	})

	// Prepare host workers for unit
	unitWorker := make([]*hostWorker, len(hosts))
	for i, host := range hosts {
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
			return errors.Wrapf(err, "host %s: init repair", host)
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

	// Calculate number of tries
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
				return errors.Wrapf(err, "host %s", worker.Host)
			}
			// check if recoverable segment repair errors occurred
			if worker.segmentErrors() > 0 {
				if run.failFast {
					return errors.Errorf("host %s: repair %d segments", worker.Host, worker.segmentErrors())
				}
				failed = true
			}
		}
		if !failed {
			return nil
		}
	}

	// Check if segment repair errors occurred and report it
	var err error
	for _, worker := range run.unitWorkers[unit] {
		if worker.segmentErrors() > 0 {
			err = multierr.Append(err, errors.Errorf("host %s: repair %d segments", worker.Host, worker.segmentErrors()))
		}
	}
	return err
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
			return "", errors.Wrap(err, "resolve coordinator DC")
		}
	default:
		dc, err = s.getCoordinatorDC(ctx, client, run.DC, ksDCs)
		if err != nil {
			return "", errors.Wrap(err, "resolve coordinator DC")
		}
	}
	return dc, nil
}

func (s *Service) getCoordinatorDC(ctx context.Context, client *scyllaclient.Client, runDCs, ksDCs []string) (string, error) {
	runSet := strset.Intersection(strset.New(runDCs...), strset.New(ksDCs...))
	if runSet.IsEmpty() {
		return "", errors.New("no matching DCs")
	}

	all, err := client.Datacenters(ctx)
	if err != nil {
		return "", errors.Wrapf(err, "get datacenters")
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
		return uuid.Nil, errors.Wrap(err, "get the cluster tokens")
	}

	return topologyHash(tokens), nil
}

// GetLastResumableRun returns the the most recent started but not done run of
// the task, if there is a recent run that is completely done ErrNotFound is
// reported.
func (s *Service) GetLastResumableRun(ctx context.Context, clusterID, taskID uuid.UUID) (*Run, error) {
	s.logger.Debug(ctx, "GetLastResumableRun",
		"cluster_id", clusterID,
		"task_id", taskID,
	)

	stmt, names := qb.Select(table.RepairRun.Name()).Where(
		qb.Eq("cluster_id"),
		qb.Eq("task_id"),
	).Limit(20).ToCql()

	q := gocqlx.Query(s.session.Query(stmt), names).BindMap(qb.M{
		"cluster_id": clusterID,
		"task_id":    taskID,
	})

	var runs []*Run
	if err := q.SelectRelease(&runs); err != nil {
		return nil, err
	}

	for _, r := range runs {
		prog, err := s.getProgress(ctx, r)
		if err != nil {
			return nil, err
		}
		p := aggregateProgress(r, prog)
		if p.segmentCount > 0 {
			if p.segmentSuccess == p.segmentCount {
				break
			}
			return r, nil
		}
	}

	return nil, service.ErrNotFound
}

// GetRun returns a run based on ID. If nothing was found mermaid.ErrNotFound
// is returned.
func (s *Service) GetRun(ctx context.Context, clusterID, taskID, runID uuid.UUID) (*Run, error) {
	s.logger.Debug(ctx, "GetRun",
		"cluster_id", clusterID,
		"task_id", taskID,
		"run_id", runID,
	)

	stmt, names := table.RepairRun.Get()

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
	stmt, names := table.RepairRun.Insert()
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
	stmt, names := table.RepairRunProgress.Select()

	q := gocqlx.Query(s.session.Query(stmt), names).BindMap(qb.M{
		"cluster_id": run.ClusterID,
		"task_id":    run.TaskID,
		"run_id":     run.ID,
	})

	var p []*RunProgress
	return p, q.SelectRelease(&p)
}

func (s *Service) getHostProgress(run *Run, unit int, host string) ([]*RunProgress, error) {
	stmt, names := table.RepairRunProgress.SelectBuilder().Where(
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

	stmt, names := table.RepairRunProgress.Insert()
	q := gocqlx.Query(s.session.Query(stmt), names).BindStruct(p)

	return q.ExecRelease()
}
