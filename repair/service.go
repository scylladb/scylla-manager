// Copyright (C) 2017 ScyllaDB

package repair

import (
	"context"
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/cespare/xxhash"
	"github.com/fatih/set"
	"github.com/gocql/gocql"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/scylladb/gocqlx"
	"github.com/scylladb/gocqlx/qb"
	log "github.com/scylladb/golog"
	"github.com/scylladb/mermaid"
	"github.com/scylladb/mermaid/cluster"
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
	s.logger.Info(ctx, "Fixing run statuses")

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
	stmt, names = schema.RepairRun.Update("status")
	u := gocqlx.Query(s.session.Query(stmt).WithContext(ctx), names)
	defer u.Release()

	for _, r := range tasks {
		if err := g.BindStruct(r).Get(r); err != nil {
			return err
		}
		if r.Status == runner.StatusRunning || r.Status == runner.StatusStopping {
			r.Status = runner.StatusStopped
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

// Repair starts an asynchronous repair process.
func (s *Service) Repair(ctx context.Context, clusterID, taskID, runID uuid.UUID, units []Unit) error {
	s.logger.Debug(ctx, "Repair",
		"cluster_id", clusterID,
		"task_id", taskID,
		"run_id", runID,
		"units", units,
	)

	run := &Run{
		ClusterID: clusterID,
		TaskID:    taskID,
		ID:        runID,
		Units:     units,
		Status:    runner.StatusRunning,
		StartTime: timeutc.Now(),
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
		return fail(mermaid.ParamError{Cause: errors.Wrap(err, "failed to load cluster")})
	}
	run.clusterName = c.String()

	// make sure no other repairs are being run on that cluster
	if err := s.tryLockCluster(run); err != nil {
		s.logger.Debug(ctx, "Lock error", "error", err)
		return fail(mermaid.ParamError{Cause: ErrActiveRepair})
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
	)

	// get the cluster client
	client, err := s.client(ctx, run.ClusterID)
	if err != nil {
		return fail(errors.Wrap(err, "failed to get the client proxy"))
	}

	// validate units
	for _, u := range run.Units {
		if err := s.validateUnit(ctx, u, client); err != nil {
			return fail(errors.Wrap(err, "invalid request"))
		}
	}

	// get last started run of the task
	prev, err := s.GetLastStartedRun(ctx, run.ClusterID, run.TaskID)
	if err != nil && err != mermaid.ErrNotFound {
		return fail(errors.Wrap(err, "failed to get previous run"))
	}
	if prev != nil {
		s.logger.Info(ctx, "Found previous run", "prev_id", prev.ID)
		switch {
		case prev.Status == runner.StatusDone:
			s.logger.Info(ctx, "Starting from scratch: previous run is done")
			prev = nil
		case timeutc.Since(prev.StartTime) > s.config.MaxRunAge:
			s.logger.Info(ctx, "Starting from scratch: previous run is too old")
			prev = nil
		}
	}
	if prev != nil {
		run.PrevID = prev.ID
		run.prevProg, err = s.getProgress(ctx, &Run{
			ClusterID: run.ClusterID,
			TaskID:    run.TaskID,
			ID:        prev.ID,
		})
		if err != nil {
			return errors.Wrap(err, "failed to get the last run progress")
		}
	}

	// get the cluster topology hash
	run.TopologyHash, err = s.topologyHash(ctx, client)
	if err != nil {
		return fail(errors.Wrap(err, "failed to get topology hash"))
	}

	// ensure topology did not change, if changed start from scratch
	if prev != nil {
		if run.TopologyHash != prev.TopologyHash {
			s.logger.Info(ctx, "Starting from scratch: topology changed")
			prev = nil
			run.PrevID = uuid.Nil
		}
	}
	if err := s.putRun(ctx, run); err != nil {
		return fail(errors.Wrap(err, "failed to update the run"))
	}

	// check the cluster partitioner
	p, err := client.Partitioner(ctx)
	if err != nil {
		return fail(errors.Wrap(err, "failed to get the client partitioner name"))
	}
	if p != scyllaclient.Murmur3Partitioner {
		return fail(errors.Errorf("unsupported partitioner %q, the only supported partitioner is %q", p, scyllaclient.Murmur3Partitioner))
	}

	// register the run
	if err := s.putRun(ctx, run); err != nil {
		return fail(errors.Wrap(err, "failed to register the run"))
	}

	// lock is used to make sure that the initialisation sequence is finished
	// before starting the repair go routine. Otherwise code optimisations
	// caused data races.
	lock := make(chan struct{})
	defer close(lock)

	// spawn async repair
	s.logger.Info(ctx, "Starting repair")

	s.wg.Add(1)
	go func() {
		// wait for unlock
		<-lock

		ctx, cancel := context.WithCancel(log.CopyTraceID(s.workerCtx, ctx))

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
				if errors.Cause(err) == errStopped {
					run.Status = runner.StatusStopped
					run.EndTime = timeutc.Now()
					s.putRunLogError(ctx, run)
				} else {
					fail(err)
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
		return errors.Errorf("empty keyspace %s", u.Keyspace)
	}
	if err := validateSubset(u.Tables, all); err != nil {
		return errors.Wrap(err, "keyspace %s missing tables")
	}

	return nil
}

func (s *Service) repairUnit(ctx context.Context, run *Run, unit int, client *scyllaclient.Client) error {
	u := run.Units[unit]

	// get the ring description
	_, ring, err := client.DescribeRing(ctx, u.Keyspace)
	if err != nil {
		return errors.Wrap(err, "failed to get the ring description")
	}

	// get local datacenter name
	dc, err := client.Datacenter(ctx)
	if err != nil {
		return errors.Wrap(err, "failed to get the local datacenter name")
	}
	s.logger.Debug(ctx, "Using DC", "dc", dc)

	// split token range into coordination hosts
	hostSegments, err := groupSegmentsByHost(dc, ring)
	if err != nil {
		return errors.Wrap(err, "segmentation failed")
	}

	// init progress
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
		prevHosts := set.NewNonTS()
		for _, p := range prog {
			prevHosts.Add(p.Host)
		}
		hosts := set.NewNonTS()
		for host := range hostSegments {
			hosts.Add(host)
		}
		if diff := set.SymmetricDifference(prevHosts, hosts); !diff.IsEmpty() {
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
			return errors.Wrapf(err, "failed to initialise the run progress %s", &p)
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

	for _, host := range hosts {
		// ensure topology did not change
		th, err := s.topologyHash(ctx, client)
		if err != nil {
			s.logger.Info(ctx, "Topology check error", "error", err)
		} else if run.TopologyHash != th {
			return errors.Errorf("topology changed old hash: %s new hash: %s", run.TopologyHash, th)
		}

		// ping host
		if _, err := client.Ping(ctx, host); err != nil {
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
			return errors.Wrapf(err, "repair error")
		}
	}
	return nil
}

func (s *Service) reportRepairProgress(ctx context.Context, run *Run) {
	t := time.NewTicker(2500 * time.Millisecond)
	defer t.Stop()

	for {
		select {
		case <-t.C:
			prog, err := s.getProgress(ctx, run)
			if err != nil {
				s.logger.Error(ctx, "Failed to get hosts progress", "error", err)
			}
			for host, percent := range hostsPercentComplete(prog) {
				repairProgress.With(prometheus.Labels{
					"cluster":  run.clusterName,
					"task":     run.TaskID.String(),
					"keyspace": run.Units[0].Keyspace, // FIXME mmt: refactor progress!!!
					"host":     host,
				}).Set(percent)
			}
		case <-ctx.Done():
			return
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
func (s *Service) GetProgress(ctx context.Context, clusterID, taskID, runID uuid.UUID) ([]*RunProgress, error) {
	s.logger.Debug(ctx, "GetProgress",
		"cluster_id", clusterID,
		"task_id", taskID,
		"run_id", runID,
	)
	return s.getProgress(ctx, &Run{ClusterID: clusterID, TaskID: taskID, ID: runID})
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
