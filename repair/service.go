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
	"github.com/scylladb/mermaid/schema"
	"github.com/scylladb/mermaid/scyllaclient"
	"github.com/scylladb/mermaid/uuid"
	"go.uber.org/atomic"
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

// FixRunStatus shall be called when the service starts to assure proper
// functioning. It iterates over all the repair units and marks running and
// stopping runs as stopped.
func (s *Service) FixRunStatus(ctx context.Context) error {
	s.logger.Info(ctx, "Fixing run statuses")

	// FIXME change impl
	//stmt, _ := qb.Select(schema.RepairUnit.Name).ToCql()
	//q := s.session.Query(stmt).WithContext(ctx)
	//defer q.Release()
	//
	//iter := gocqlx.Iter(q)
	//defer iter.Close()
	//
	//var u Unit
	//for iter.StructScan(&u) {
	//	last, err := s.GetLastRun(ctx, &u)
	//	if err == mermaid.ErrNotFound {
	//		continue
	//	}
	//	if err != nil {
	//		return errors.Wrap(err, "failed to get last run of a unit")
	//	}
	//
	//	switch last.Status {
	//	case StatusRunning, StatusStopping:
	//		last.Status = StatusStopped
	//		if err := s.putRun(ctx, last); err != nil {
	//			return errors.Wrap(err, "failed to update a run")
	//		}
	//		s.logger.Info(ctx, "Marked run as stopped",
	//			"cluster_id", last.ClusterID,
	//			"task_id", u.ID,
	//			"run_id", last.ID,
	//		)
	//	}
	//}
	//
	//s.logger.Info(ctx, "Done")
	//
	//return iter.Close()
	return nil
}

// Repair starts an asynchronous repair process.
func (s *Service) Repair(ctx context.Context, clusterID, taskID, runID uuid.UUID, u Unit) error {
	s.logger.Debug(ctx, "Repair",
		"cluster_id", clusterID,
		"task_id", taskID,
		"run_id", runID,
		"unit", u,
	)

	run := &Run{
		ClusterID: clusterID,
		TaskID:    taskID,
		ID:        runID,
		Keyspace:  u.Keyspace,
		Tables:    u.Tables,
		Status:    StatusRunning,
		StartTime: timeutc.Now(),
	}

	// fail updates a run and passes the error
	fail := func(err error) error {
		run.Status = StatusError
		run.Cause = err.Error()
		run.EndTime = timeutc.Now()
		s.putRunLogError(ctx, run)
		return err
	}

	// get cluster
	c, err := s.cluster(ctx, run.ClusterID)
	if err != nil {
		return fail(mermaid.ParamError{Cause: errors.Wrap(err, "client not found")})
	}

	// lock is used to make sure that the initialisation sequence is finished
	// before starting the repair go routine.
	lock := atomic.NewBool(true)
	defer lock.Store(false)

	// make sure no other repairs are being run on that cluster
	if err := s.tryLockCluster(run); err != nil {
		s.logger.Debug(ctx, "Lock error", "error", err)
		return fail(mermaid.ParamError{Cause: ErrActiveRepair})
	}
	defer func() {
		if run.Status != StatusRunning {
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

	// get last started run of the unit
	prev, err := s.GetLastStartedRun(ctx, run.ClusterID, run.TaskID)
	if err != nil && err != mermaid.ErrNotFound {
		return fail(errors.Wrap(err, "failed to get previous run"))
	}
	if prev != nil {
		s.logger.Info(ctx, "Found previous run", "prev_id", prev.ID)
		switch {
		case prev.Status == StatusDone:
			s.logger.Info(ctx, "Starting from scratch: previous run is done")
			prev = nil
		case timeutc.Since(prev.StartTime) > s.config.MaxRunAge:
			s.logger.Info(ctx, "Starting from scratch: previous run is too old")
			prev = nil
		}
	}
	if prev != nil {
		run.PrevID = prev.ID
	}

	// register the run
	if err := s.putRun(ctx, run); err != nil {
		return fail(errors.Wrap(err, "failed to register the run"))
	}

	// get the cluster client
	client, err := s.client(ctx, run.ClusterID)
	if err != nil {
		return fail(errors.Wrap(err, "failed to get the client proxy"))
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

	// check keyspace and tables
	all, err := client.Tables(ctx, run.Keyspace)
	if err != nil {
		return fail(errors.Wrap(err, "failed to get the client table names for keyspace"))
	}
	if len(all) == 0 {
		return fail(errors.Errorf("missing or empty keyspace %q", run.Keyspace))
	}
	if err := validateTables(run.Tables, all); err != nil {
		return fail(errors.Wrapf(err, "keyspace %q", run.Keyspace))
	}

	// check the cluster partitioner
	p, err := client.Partitioner(ctx)
	if err != nil {
		return fail(errors.Wrap(err, "failed to get the client partitioner name"))
	}
	if p != scyllaclient.Murmur3Partitioner {
		return fail(errors.Errorf("unsupported partitioner %q, the only supported partitioner is %q", p, scyllaclient.Murmur3Partitioner))
	}

	// get the ring description
	_, ring, err := client.DescribeRing(ctx, u.Keyspace)
	if err != nil {
		return fail(errors.Wrap(err, "failed to get the ring description"))
	}

	// get local datacenter name
	dc, err := client.Datacenter(ctx)
	if err != nil {
		return fail(errors.Wrap(err, "failed to get the local datacenter name"))
	}
	s.logger.Debug(ctx, "Using DC", "dc", dc)

	// split token range into coordination hosts
	hostSegments, err := groupSegmentsByHost(dc, ring)
	if err != nil {
		return fail(errors.Wrap(err, "segmentation failed"))
	}

	// init empty progress
	for host := range hostSegments {
		p := RunProgress{
			ClusterID: run.ClusterID,
			TaskID:    run.TaskID,
			RunID:     run.ID,
			Host:      host,
		}
		if err := s.putRunProgress(ctx, &p); err != nil {
			return fail(errors.Wrapf(err, "failed to initialise the run progress %s", &p))
		}

		l := prometheus.Labels{
			"cluster": c.String(),
			"unit":    run.TaskID.String(),
			"host":    host,
			"shard":   "0",
		}

		repairSegmentsTotal.With(l).Set(0)
		repairSegmentsSuccess.With(l).Set(0)
		repairSegmentsError.With(l).Set(0)
	}

	// update progress from the previous run
	if prev != nil {
		prog, err := s.GetProgress(ctx, &Run{
			ClusterID: run.ClusterID,
			TaskID:    run.TaskID,
			ID:        run.PrevID,
		})
		if err != nil {
			return fail(errors.Wrap(err, "failed to get the last run progress"))
		}

		// check if host did not change
		prevHosts := set.NewNonTS()
		for _, p := range prog {
			prevHosts.Add(p.Host)
		}
		hosts := set.NewNonTS()
		for host := range hostSegments {
			hosts.Add(host)
		}

		if diff := set.SymmetricDifference(prevHosts, hosts); !diff.IsEmpty() {
			s.logger.Info(ctx, "Starting from scratch: hosts changed",
				"old", prevHosts,
				"new", hosts,
				"diff", diff,
			)

			prev = nil
			run.PrevID = uuid.Nil
			if err := s.putRun(ctx, run); err != nil {
				return fail(errors.Wrap(err, "failed to update the run"))
			}
		} else {
			for _, p := range prog {
				if p.started() {
					p.RunID = run.ID
					if err := s.putRunProgress(ctx, p); err != nil {
						return fail(errors.Wrapf(err, "failed to initialise the run progress %s", &p))
					}

					l := prometheus.Labels{
						"cluster": c.String(),
						"unit":    run.TaskID.String(),
						"host":    p.Host,
						"shard":   fmt.Sprint(p.Shard),
					}

					repairSegmentsTotal.With(l).Set(float64(p.SegmentCount))
					repairSegmentsSuccess.With(l).Set(float64(p.SegmentSuccess))
					repairSegmentsError.With(l).Set(float64(p.SegmentError))
				}
			}
		}
	}

	// spawn async repair
	s.logger.Info(ctx, "Starting repair")

	s.wg.Add(1)
	go func() {
		// wait for unlock
		for lock.Load() {
			time.Sleep(50 * time.Millisecond)
		}

		ctx, cancel := context.WithCancel(log.CopyTraceID(s.workerCtx, ctx))

		defer func() {
			s.wg.Done()
			if v := recover(); v != nil {
				s.logger.Error(ctx, "Panic", "panic", v)
				fail(errors.Errorf("%s", v))
			}
			if err := s.unlockCluster(run); err != nil {
				s.logger.Error(ctx, "Unlock error", "error", err)
			}
			cancel()
		}()

		go s.reportRepairProgress(ctx, c, run)

		if err := s.repair(ctx, c, run, client, hostSegments); err != nil {
			fail(err)
		}

		s.logger.Info(ctx, "Status", "status", run.Status)
	}()

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

func (s *Service) repair(ctx context.Context, c *cluster.Cluster, run *Run, client *scyllaclient.Client, hostSegments map[string][]*Segment) error {
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
			Cluster:  c,
			Run:      run,
			Host:     host,
			Segments: hostSegments[host],

			Service: s,
			Client:  client,
			Logger:  s.logger.Named("worker").With("host", host),
		}
		if err := w.exec(ctx); err != nil {
			return errors.Wrapf(err, "repair error")
		}

		if ctx.Err() != nil {
			return nil
		}

		stopped, err := s.isStopped(ctx, run)
		if err != nil {
			w.Logger.Error(ctx, "Service error", "error", err)
		}

		if stopped {
			run.Status = StatusStopped
			run.EndTime = timeutc.Now()
			s.putRunLogError(ctx, run)

			return nil
		}
	}

	run.Status = StatusDone
	run.EndTime = timeutc.Now()
	s.putRunLogError(ctx, run)

	return nil
}

func (s *Service) reportRepairProgress(ctx context.Context, c *cluster.Cluster, run *Run) {
	t := time.NewTicker(2500 * time.Millisecond)
	defer t.Stop()

	for {
		select {
		case <-t.C:
			prog, err := s.getAllHostsProgress(ctx, run)
			if err != nil {
				s.logger.Error(ctx, "Failed to get hosts progress", "error", err)
			}
			for host, percent := range hostsPercentComplete(prog) {
				repairProgress.With(prometheus.Labels{
					"cluster": c.String(),
					"unit":    run.TaskID.String(),
					"host":    host,
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

// GetLastRun returns the the most recent run of the unit.
func (s *Service) GetLastRun(ctx context.Context, clusterID, taskID uuid.UUID) (*Run, error) {
	s.logger.Debug(ctx, "GetLastRun",
		"cluster_id", clusterID,
		"task_id", taskID,
	)

	stmt, names := qb.Select(schema.RepairRun.Name).Where(
		qb.Eq("cluster_id"), qb.Eq("task_id"),
	).Limit(1).ToCql()

	q := gocqlx.Query(s.session.Query(stmt).WithContext(ctx), names).BindMap(qb.M{
		"cluster_id": clusterID,
		"task_id":    taskID,
	})
	defer q.Release()

	if q.Err() != nil {
		return nil, q.Err()
	}

	var r Run
	if err := gocqlx.Get(&r, q.Query); err != nil {
		return nil, err
	}

	return &r, nil
}

// GetLastStartedRun returns the the most recent run of the unit that started
// the repair.
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
		if r.Status != StatusError {
			return r, nil
		}

		// check if repair started
		p, err := s.getAllHostsProgress(ctx, r)
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
	defer q.Release()

	if q.Err() != nil {
		return nil, q.Err()
	}

	var r Run
	if err := gocqlx.Get(&r, q.Query); err != nil {
		return nil, err
	}

	return &r, nil
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

	if r.Status != StatusRunning {
		return errors.New("not running")
	}

	s.logger.Info(ctx, "Stopping repair",
		"cluster_id", clusterID,
		"task_id", taskID,
		"run_id", runID,
	)

	r.Status = StatusStopping

	return s.putRun(ctx, r)
}

// isStopped checks if repair is in StatusStopping or StatusStopped.
func (s *Service) isStopped(ctx context.Context, run *Run) (bool, error) {
	stmt, names := schema.RepairRun.Get("status")
	q := gocqlx.Query(s.session.Query(stmt).WithContext(ctx), names).BindStruct(run)
	if q.Err() != nil {
		return false, q.Err()
	}

	var v Status
	if err := q.Query.Scan(&v); err != nil {
		return false, err
	}

	return v == StatusStopping || v == StatusStopped, nil
}

// GetProgress returns run progress. If nothing was found mermaid.ErrNotFound
// is returned.
func (s *Service) GetProgress(ctx context.Context, run *Run, hosts ...string) ([]*RunProgress, error) {
	s.logger.Debug(ctx, "GetProgress",
		"cluster_id", run.ClusterID,
		"task_id", run.TaskID,
		"run_id", run.ID,
	)

	if len(hosts) == 0 {
		return s.getAllHostsProgress(ctx, run)
	}

	return s.getHostProgress(ctx, run, hosts...)
}

func (s *Service) getAllHostsProgress(ctx context.Context, run *Run) ([]*RunProgress, error) {
	stmt, names := schema.RepairRunProgress.Select()
	q := gocqlx.Query(s.session.Query(stmt).WithContext(ctx), names)
	defer q.Release()

	q.BindMap(qb.M{
		"cluster_id": run.ClusterID,
		"task_id":    run.TaskID,
		"run_id":     run.ID,
	})
	if q.Err() != nil {
		return nil, q.Err()
	}

	var p []*RunProgress
	return p, gocqlx.Select(&p, q.Query)
}

func (s *Service) getHostProgress(ctx context.Context, run *Run, hosts ...string) ([]*RunProgress, error) {
	stmt, names := schema.RepairRunProgress.SelectBuilder().Where(
		qb.Eq("host"),
	).ToCql()

	q := gocqlx.Query(s.session.Query(stmt).WithContext(ctx), names)
	defer q.Release()

	m := qb.M{
		"cluster_id": run.ClusterID,
		"task_id":    run.TaskID,
		"run_id":     run.ID,
	}

	var p []*RunProgress

	for _, h := range hosts {
		m["host"] = h

		q.BindMap(m)
		if q.Err() != nil {
			return nil, q.Err()
		}

		var v []*RunProgress
		if err := gocqlx.Select(&v, q.Query); err != nil {
			return nil, err
		}

		p = append(p, v...)
	}

	return p, nil
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

// FIXME change getHostProgress to accept signle host, remove/refactor GetProgress
// FIXME change API "clusterID, taskID, runID uuid.UUID"?
// FIXME StopRepair do update: qb.Update(schema.RepairRun.Name).SetLit("status", StatusStopping.String())
