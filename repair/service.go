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
	"github.com/scylladb/gocqlx"
	"github.com/scylladb/gocqlx/qb"
	"github.com/scylladb/mermaid"
	"github.com/scylladb/mermaid/log"
	"github.com/scylladb/mermaid/sched/runner"
	"github.com/scylladb/mermaid/schema"
	"github.com/scylladb/mermaid/scylla"
	"github.com/scylladb/mermaid/uuid"
)

// globalClusterID is a special value used as a cluster ID for a global
// configuration.
var globalClusterID = uuid.NewFromUint64(0, 0)

// Service orchestrates cluster repairs.
type Service struct {
	session      *gocql.Session
	client       scylla.ProviderFunc
	workerCtx    context.Context
	workerCancel context.CancelFunc
	wg           sync.WaitGroup
	logger       log.Logger
}

// NewService creates a new service instance.
func NewService(session *gocql.Session, p scylla.ProviderFunc, l log.Logger) (*Service, error) {
	if session == nil || session.Closed() {
		return nil, errors.New("invalid session")
	}

	if p == nil {
		return nil, errors.New("invalid scylla provider")
	}

	ctx, cancel := context.WithCancel(context.Background())

	return &Service{
		session:      session,
		client:       p,
		workerCtx:    ctx,
		workerCancel: cancel,
		logger:       l,
	}, nil
}

// FixRunStatus shall be called when the service starts to assure proper
// functioning. It iterates over all the repair units and marks running and
// stopping runs as stopped.
func (s *Service) FixRunStatus(ctx context.Context) error {
	s.logger.Debug(ctx, "FixRunStatus")

	stmt, _ := qb.Select(schema.RepairUnit.Name).ToCql()
	iter := gocqlx.Iter(s.session.Query(stmt).WithContext(ctx))

	defer func() {
		iter.Close()
		iter.ReleaseQuery()
	}()

	var u Unit
	for iter.StructScan(&u) {
		last, err := s.GetLastRun(ctx, &u)
		if err == mermaid.ErrNotFound {
			continue
		}
		if err != nil {
			return errors.Wrap(err, "failed to get last run of a unit")
		}

		switch last.Status {
		case StatusRunning, StatusStopping:
			last.Status = StatusStopped
			if err := s.putRun(ctx, last); err != nil {
				return errors.Wrap(err, "failed to update a run")
			}
			s.logger.Info(ctx, "Marked run as stopped", "unit", u, "task_id", last.ID)
		}
	}

	return iter.Close()
}

// Repair starts an asynchronous repair process.
func (s *Service) Repair(ctx context.Context, u *Unit, taskID uuid.UUID) error {
	s.logger.Debug(ctx, "Repair", "unit", u, "task_id", taskID)

	// validate the unit
	if err := u.Validate(); err != nil {
		return errors.Wrap(err, "invalid unit")
	}

	// get the unit configuration
	c, err := s.GetMergedUnitConfig(ctx, u)
	if err != nil {
		return errors.Wrap(err, "failed to get a unit configuration")
	}
	s.logger.Debug(ctx, "Using config", "config", &c.Config)

	// if repair is disabled return an error
	if !*c.Config.Enabled {
		return ErrDisabled
	}

	// get last run of the unit
	prev, err := s.GetLastRun(ctx, u)
	if err != nil && err != mermaid.ErrNotFound {
		return errors.Wrap(err, "failed to get last run of the unit")
	}

	if prev != nil {
		switch prev.Status {
		case StatusRunning, StatusStopping:
			return errors.Errorf("repair in progress %s", prev.ID)
		case StatusDone, StatusError:
			prev = nil
		}
	}

	// register a run with preparing status
	r := Run{
		ClusterID: u.ClusterID,
		UnitID:    u.ID,
		ID:        taskID,
		Keyspace:  u.Keyspace,
		Tables:    u.Tables,
		Status:    StatusRunning,
		StartTime: time.Now(),
	}
	if prev != nil {
		r.PrevID = prev.ID
	}
	if err := s.putRun(ctx, &r); err != nil {
		errors.Wrap(err, "failed to register the run")
	}

	// fail updates a run and passes the error
	fail := func(err error) error {
		r.Status = StatusError
		r.Cause = err.Error()
		r.EndTime = time.Now()
		s.putRunLogError(ctx, &r)
		return err
	}

	// get the cluster client
	cluster, err := s.client(ctx, u.ClusterID)
	if err != nil {
		return fail(errors.Wrap(err, "failed to get the cluster proxy"))
	}

	// get the cluster topology hash
	tokens, err := cluster.Tokens(ctx)
	if err != nil {
		return fail(errors.Wrap(err, "failed to get the cluster tokens"))
	}

	// ensure topology did not change, if changed start from scratch
	r.TopologyHash = topologyHash(tokens)
	if prev != nil {
		if r.TopologyHash != prev.TopologyHash {
			s.logger.Info(ctx, "Starting from scratch: topology changed",
				"task_id", r.ID,
				"prev_task_id", prev.ID,
			)
			prev = nil
			r.PrevID = uuid.Nil
		}
	}
	if err := s.putRun(ctx, &r); err != nil {
		return fail(errors.Wrap(err, "failed to update the run"))
	}

	// check keyspace and tables
	all, err := cluster.Tables(ctx, r.Keyspace)
	if err != nil {
		return fail(errors.Wrap(err, "failed to get the cluster table names for keyspace"))
	}
	if len(all) == 0 {
		return fail(errors.Errorf("missing or empty keyspace %q", r.Keyspace))
	}
	if err := validateTables(r.Tables, all); err != nil {
		return fail(errors.Wrapf(err, "keyspace %q", r.Keyspace))
	}

	// check the cluster partitioner
	p, err := cluster.Partitioner(ctx)
	if err != nil {
		return fail(errors.Wrap(err, "failed to get the cluster partitioner name"))
	}
	if p != scylla.Murmur3Partitioner {
		return fail(errors.Errorf("unsupported partitioner %q, the only supported partitioner is %q", p, scylla.Murmur3Partitioner))
	}

	// get the ring description
	_, ring, err := cluster.DescribeRing(ctx, u.Keyspace)
	if err != nil {
		return fail(errors.Wrap(err, "failed to get the ring description"))
	}

	// get local datacenter name
	dc, err := cluster.Datacenter(ctx)
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
			ClusterID: r.ClusterID,
			UnitID:    r.UnitID,
			RunID:     r.ID,
			Host:      host,
		}
		if err := s.putRunProgress(ctx, &p); err != nil {
			return fail(errors.Wrapf(err, "failed to initialise the run progress %s", &p))
		}
	}

	// update progress from the previous run
	if prev != nil {
		prog, err := s.GetProgress(ctx, u, prev.ID)
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
			s.logger.Info(ctx, "Starting from scratch: hosts changed check that all API hosts belong to the same DC",
				"task_id", r.ID,
				"prev_task_id", prev.ID,
				"old", prevHosts,
				"new", hosts,
				"diff", diff,
			)

			prev = nil
			r.PrevID = uuid.Nil
			if err := s.putRun(ctx, &r); err != nil {
				return fail(errors.Wrap(err, "failed to update the run"))
			}
		} else {
			for _, p := range prog {
				if p.started() {
					p.RunID = r.ID
					if err := s.putRunProgress(ctx, p); err != nil {
						return fail(errors.Wrapf(err, "failed to initialise the run progress %s", &p))
					}
				}
			}
		}
	}

	// spawn async repair
	wctx := log.WithTraceID(s.workerCtx)
	s.logger.Info(ctx, "Starting repair",
		"unit", u,
		"task_id", taskID,
		"prev_task_id", r.PrevID,
		"worker_trace_id", log.TraceID(wctx),
	)
	s.wg.Add(1)
	go func() {
		defer func() {
			s.wg.Done()
			if v := recover(); v != nil {
				s.logger.Error(wctx, "Panic", "panic", v)
				fail(errors.Errorf("%s", v))
			}
		}()
		s.repair(wctx, u, &r, &c.Config, cluster, hostSegments)
	}()

	return nil
}

func (s *Service) repair(ctx context.Context, u *Unit, r *Run, c *Config, cluster *scylla.Client, hostSegments map[string][]*Segment) {
	// shuffle hosts
	hosts := make([]string, 0, len(hostSegments))
	for host := range hostSegments {
		hosts = append(hosts, host)
	}
	sort.Slice(hosts, func(i, j int) bool {
		return xxhash.Sum64String(hosts[i]) < xxhash.Sum64String(hosts[j])
	})

	for _, host := range hosts {
		w := worker{
			Unit:     u,
			Run:      r,
			Config:   c,
			Service:  s,
			Cluster:  cluster,
			Host:     host,
			Segments: hostSegments[host],

			logger: s.logger.Named("worker").With("task_id", r.ID, "host", host),
		}
		if err := w.exec(ctx); err != nil {
			s.logger.Error(ctx, "Worker exec error", "error", err)
		}

		if ctx.Err() != nil {
			s.logger.Info(ctx, "Aborted", "task_id", r.ID)
			return
		}

		stopped, err := s.isStopped(ctx, u, r.ID)
		if err != nil {
			w.logger.Error(ctx, "Service error", "error", err)
		}

		if stopped {
			r.Status = StatusStopped
			r.EndTime = time.Now()
			s.putRunLogError(ctx, r)

			s.logger.Info(ctx, "Stopped", "task_id", r.ID)
			return
		}
	}

	r.Status = StatusDone
	r.EndTime = time.Now()
	s.putRunLogError(ctx, r)

	s.logger.Info(ctx, "Done", "task_id", r.ID)
}

// ListRuns returns runs for the unit in descending order, latest runs first.
func (s *Service) ListRuns(ctx context.Context, u *Unit, f *RunFilter) ([]*Run, error) {
	s.logger.Debug(ctx, "ListRuns", "unit", u, "filter", f)

	// validate the unit
	if err := u.Validate(); err != nil {
		return nil, errors.Wrap(err, "invalid unit")
	}

	// validate the filter
	if err := f.Validate(); err != nil {
		return nil, errors.Wrap(err, "invalid filter")
	}

	sel := qb.Select(schema.RepairRun.Name).Where(
		qb.Eq("cluster_id"),
		qb.Eq("unit_id"),
	)
	if f.Limit != 0 {
		sel.Limit(f.Limit)
	}

	stmt, names := sel.ToCql()

	q := gocqlx.Query(s.session.Query(stmt).WithContext(ctx), names).BindMap(qb.M{
		"cluster_id": u.ClusterID,
		"unit_id":    u.ID,
	})
	if q.Err() != nil {
		return nil, q.Err()
	}

	var v []*Run
	if err := gocqlx.Select(&v, q.Query); err != nil {
		return nil, err
	}

	return v, nil
}

// GetLastRun returns the the most recent run of the unit.
func (s *Service) GetLastRun(ctx context.Context, u *Unit) (*Run, error) {
	s.logger.Debug(ctx, "GetLastRun", "unit", u)

	// validate the unit
	if err := u.Validate(); err != nil {
		return nil, errors.Wrap(err, "invalid unit")
	}

	stmt, names := qb.Select(schema.RepairRun.Name).
		Where(
			qb.Eq("cluster_id"),
			qb.Eq("unit_id"),
		).Limit(1).
		ToCql()

	q := gocqlx.Query(s.session.Query(stmt).WithContext(ctx), names).BindMap(qb.M{
		"cluster_id": u.ClusterID,
		"unit_id":    u.ID,
	})
	if q.Err() != nil {
		return nil, q.Err()
	}

	var r Run
	if err := gocqlx.Get(&r, q.Query); err != nil {
		return nil, err
	}

	return &r, nil
}

// GetRun returns a run based on ID. If nothing was found mermaid.ErrNotFound
// is returned.
func (s *Service) GetRun(ctx context.Context, u *Unit, taskID uuid.UUID) (*Run, error) {
	s.logger.Debug(ctx, "GetRun", "unit", u, "task_id", taskID)

	// validate the unit
	if err := u.Validate(); err != nil {
		return nil, errors.Wrap(err, "invalid unit")
	}

	stmt, names := schema.RepairRun.Get()

	q := gocqlx.Query(s.session.Query(stmt).WithContext(ctx), names).BindMap(qb.M{
		"cluster_id": u.ClusterID,
		"unit_id":    u.ID,
		"id":         taskID,
	})
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
	s.logger.Debug(ctx, "PutRun", "run", r)

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

// StopRun marks a running repair as stopping.
func (s *Service) StopRun(ctx context.Context, u *Unit, taskID uuid.UUID) error {
	s.logger.Debug(ctx, "StopRun", "unit", u, "task_id", taskID)

	// validate the unit
	if err := u.Validate(); err != nil {
		return errors.Wrap(err, "invalid unit")
	}

	r, err := s.GetRun(ctx, u, taskID)
	if err != nil {
		return err
	}

	if r.Status != StatusRunning {
		return errors.New("not running")
	}

	r.Status = StatusStopping

	return s.putRun(ctx, r)
}

// isStopped checks if repair is in StatusStopping or StatusStopped.
func (s *Service) isStopped(ctx context.Context, u *Unit, taskID uuid.UUID) (bool, error) {
	s.logger.Debug(ctx, "isStopped", "unit", u, "task_id", taskID)

	stmt, names := schema.RepairRun.Select("status")
	q := gocqlx.Query(s.session.Query(stmt).WithContext(ctx), names).BindMap(qb.M{
		"cluster_id": u.ClusterID,
		"unit_id":    u.ID,
		"id":         taskID,
	})
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
func (s *Service) GetProgress(ctx context.Context, u *Unit, taskID uuid.UUID, hosts ...string) ([]*RunProgress, error) {
	s.logger.Debug(ctx, "GetProgress", "unit", u, "task_id", taskID)

	// validate the unit
	if err := u.Validate(); err != nil {
		return nil, errors.Wrap(err, "invalid unit")
	}

	t := schema.RepairRunProgress
	b := qb.Select(t.Name).Where(t.PrimaryKey[0:len(t.PartKey)]...)
	if len(hosts) > 0 {
		b.Where(qb.In("host"))
	}

	stmt, names := b.ToCql()

	q := gocqlx.Query(s.session.Query(stmt).WithContext(ctx), names).BindMap(qb.M{
		"cluster_id": u.ClusterID,
		"unit_id":    u.ID,
		"run_id":     taskID,
		"host":       hosts,
	})
	if q.Err() != nil {
		return nil, q.Err()
	}

	var v []*RunProgress
	if err := gocqlx.Select(&v, q.Query); err != nil {
		return nil, err
	}

	return v, nil
}

// putRunProgress upserts a repair run.
func (s *Service) putRunProgress(ctx context.Context, p *RunProgress) error {
	s.logger.Debug(ctx, "PutRunProgress", "run_progress", p)

	stmt, names := schema.RepairRunProgress.Insert()
	q := gocqlx.Query(s.session.Query(stmt).WithContext(ctx), names).BindStruct(p)

	return q.ExecRelease()
}

// GetMergedUnitConfig returns a merged configuration for a unit.
// The configuration has no nil values. If any of the source configurations are
// disabled the resulting configuration is disabled. For other fields first
// matching configuration is used.
func (s *Service) GetMergedUnitConfig(ctx context.Context, u *Unit) (*ConfigInfo, error) {
	s.logger.Debug(ctx, "GetMergedUnitConfig", "unit", u)

	// validate the unit
	if err := u.Validate(); err != nil {
		return nil, errors.Wrap(err, "invalid unit")
	}

	order := []ConfigSource{
		{
			ClusterID:  u.ClusterID,
			Type:       UnitConfig,
			ExternalID: u.ID.String(),
		},
		{
			ClusterID:  u.ClusterID,
			Type:       KeyspaceConfig,
			ExternalID: u.Keyspace,
		},
		{
			ClusterID: u.ClusterID,
			Type:      ClusterConfig,
		},
		{
			ClusterID: globalClusterID,
			Type:      tenantConfig,
		},
	}

	all := make([]*Config, 0, len(order))
	src := order[:]

	for _, o := range order {
		c, err := s.GetConfig(ctx, o)
		// no entry
		if err == mermaid.ErrNotFound {
			continue
		}
		if err != nil {
			return nil, err
		}

		// add result
		all = append(all, c)
		src = append(src, o)
	}

	return mergeConfigs(all, src)
}

// GetConfig returns repair configuration for a given object. If nothing was
// found mermaid.ErrNotFound is returned.
func (s *Service) GetConfig(ctx context.Context, src ConfigSource) (*Config, error) {
	s.logger.Debug(ctx, "GetConfig", "source", src)

	stmt, names := schema.RepairConfig.Get()

	q := gocqlx.Query(s.session.Query(stmt).WithContext(ctx), names).BindStruct(src)
	if q.Err() != nil {
		return nil, q.Err()
	}

	var c Config
	if err := gocqlx.Iter(q.Query).Unsafe().Get(&c); err != nil {
		return nil, err
	}

	return &c, nil
}

// PutConfig upserts repair configuration for a given object.
func (s *Service) PutConfig(ctx context.Context, src ConfigSource, c *Config) error {
	s.logger.Debug(ctx, "PutConfig", "source", src, "config", c)

	if err := c.Validate(); err != nil {
		return err
	}

	stmt, names := schema.RepairConfig.Insert()

	q := gocqlx.Query(s.session.Query(stmt).WithContext(ctx), names).BindStructMap(c, qb.M{
		"cluster_id":  src.ClusterID,
		"type":        src.Type,
		"external_id": src.ExternalID,
	})

	return q.ExecRelease()
}

// DeleteConfig removes repair configuration for a given object.
func (s *Service) DeleteConfig(ctx context.Context, src ConfigSource) error {
	s.logger.Debug(ctx, "DeleteConfig", "source", src)

	stmt, names := schema.RepairConfig.Delete()
	q := gocqlx.Query(s.session.Query(stmt).WithContext(ctx), names).BindStruct(src)

	return q.ExecRelease()
}

// SyncUnits ensures that for every keyspace there is a Unit. If there is no
// unit it will be created
func (s *Service) SyncUnits(ctx context.Context, clusterID uuid.UUID) error {
	s.logger.Debug(ctx, "SyncUnits", "cluster_id", clusterID)

	// get the cluster client
	cluster, err := s.client(ctx, clusterID)
	if err != nil {
		return errors.Wrap(err, "failed to get the cluster proxy")
	}

	// cluster keyspaces
	keyspaces, err := cluster.Keyspaces(ctx)
	if err != nil {
		return errors.Wrap(err, "failed to list keyspaces")
	}

	ck := set.NewNonTS()
	for _, k := range keyspaces {
		ck.Add(k)
	}

	// database keyspaces
	units, err := s.ListUnits(ctx, clusterID, &UnitFilter{})
	if err != nil {
		return errors.Wrap(err, "failed to list units")
	}

	dbk := set.NewNonTS()
	for _, u := range units {
		dbk.Add(u.Keyspace)
	}

	var dbErr error

	// add missing keyspaces
	set.Difference(ck, dbk).Each(func(i interface{}) bool {
		dbErr = s.PutUnit(ctx, &Unit{ClusterID: clusterID, Keyspace: i.(string)})
		return dbErr == nil
	})
	if dbErr != nil {
		return dbErr
	}

	// delete dropped keyspaces
	set.Difference(dbk, ck).Each(func(i interface{}) bool {
		k := i.(string)
		for _, u := range units {
			if u.Keyspace == k {
				dbErr = s.DeleteUnit(ctx, clusterID, u.ID)
				if dbErr != nil {
					return false
				}
			}
		}
		return true
	})

	return dbErr
}

// ListUnits returns all the units in the cluster.
func (s *Service) ListUnits(ctx context.Context, clusterID uuid.UUID, f *UnitFilter) ([]*Unit, error) {
	s.logger.Debug(ctx, "ListUnits", "cluster_id", clusterID, "filter", f)

	// validate the filter
	if err := f.Validate(); err != nil {
		return nil, errors.Wrap(err, "invalid filter")
	}

	stmt, names := schema.RepairUnit.Select()

	q := gocqlx.Query(s.session.Query(stmt).WithContext(ctx), names).BindMap(qb.M{
		"cluster_id": clusterID,
	})
	if q.Err() != nil {
		return nil, q.Err()
	}

	var units []*Unit
	if err := gocqlx.Select(&units, q.Query); err != nil {
		return nil, err
	}

	// nothing to filter
	if f.Name == "" {
		return units, nil
	}

	filtered := units[:0]
	for _, u := range units {
		if u.Name == f.Name {
			filtered = append(filtered, u)
		}
	}
	for i := len(filtered); i < len(units); i++ {
		units[i] = nil
	}

	return filtered, nil
}

// GetUnit returns repair unit based on ID or name. If nothing was found
// mermaid.ErrNotFound is returned.
func (s *Service) GetUnit(ctx context.Context, clusterID uuid.UUID, idOrName string) (*Unit, error) {
	var id uuid.UUID

	if err := id.UnmarshalText([]byte(idOrName)); err == nil {
		return s.GetUnitByID(ctx, clusterID, id)
	}

	return s.GetUnitByName(ctx, clusterID, idOrName)
}

// GetUnitByID returns repair unit based on ID. If nothing was found
// mermaid.ErrNotFound is returned.
func (s *Service) GetUnitByID(ctx context.Context, clusterID, id uuid.UUID) (*Unit, error) {
	s.logger.Debug(ctx, "GetUnitByID", "cluster_id", clusterID, "id", id)

	stmt, names := schema.RepairUnit.Get()

	q := gocqlx.Query(s.session.Query(stmt).WithContext(ctx), names).BindMap(qb.M{
		"cluster_id": clusterID,
		"id":         id,
	})
	if q.Err() != nil {
		return nil, q.Err()
	}

	var u Unit
	if err := gocqlx.Get(&u, q.Query); err != nil {
		return nil, err
	}

	return &u, nil
}

// GetUnitByName returns repair unit based on name. If nothing was found
// mermaid.ErrNotFound is returned.
func (s *Service) GetUnitByName(ctx context.Context, clusterID uuid.UUID, name string) (*Unit, error) {
	s.logger.Debug(ctx, "GetUnitByName", "cluster_id", clusterID, "name", name)

	units, err := s.ListUnits(ctx, clusterID, &UnitFilter{Name: name})
	if err != nil {
		return nil, err
	}

	switch len(units) {
	case 0:
		return nil, mermaid.ErrNotFound
	case 1:
		return units[0], nil
	default:
		return nil, errors.Errorf("multiple units share the same name %q", name)
	}
}

// PutUnit upserts a repair unit, unit instance must pass Validate() checks.
// If u.ID == uuid.Nil a new one is generated.
func (s *Service) PutUnit(ctx context.Context, u *Unit) error {
	s.logger.Debug(ctx, "PutUnit", "unit", u)
	if u == nil {
		return errors.New("nil unit")
	}

	if u.ID == uuid.Nil {
		var err error
		if u.ID, err = uuid.NewRandom(); err != nil {
			return errors.Wrap(err, "couldn't generate random UUID for Unit")
		}
	}

	// validate the unit
	if err := u.Validate(); err != nil {
		return err
	}

	// check for conflicting names
	if u.Name != "" {
		conflict, err := s.GetUnitByName(ctx, u.ClusterID, u.Name)
		if err != mermaid.ErrNotFound {
			if err != nil {
				return err
			}
			if conflict.ID != u.ID {
				return errors.Errorf("name conflict on %q", u.Name)
			}
		}
	}

	stmt, names := schema.RepairUnit.Insert()
	q := gocqlx.Query(s.session.Query(stmt).WithContext(ctx), names).BindStruct(u)

	return q.ExecRelease()
}

// DeleteUnit removes repair based on ID.
func (s *Service) DeleteUnit(ctx context.Context, clusterID, id uuid.UUID) error {
	s.logger.Debug(ctx, "DeleteUnit", "cluster_id", clusterID, "id", id)

	stmt, names := schema.RepairUnit.Delete()

	q := gocqlx.Query(s.session.Query(stmt).WithContext(ctx), names).BindMap(qb.M{
		"cluster_id": clusterID,
		"id":         id,
	})

	return q.ExecRelease()
}

// Close terminates all the worker routines.
func (s *Service) Close(ctx context.Context) {
	s.logger.Info(ctx, "Waiting for workers to exit")
	s.workerCancel()
	s.wg.Wait()
}

// implement sched/runner.Runner

// RunTask implements sched/runner.Runner.
func (s *Service) RunTask(ctx context.Context, clusterID, taskID uuid.UUID, props runner.TaskProperties) error {
	u, err := s.GetUnit(ctx, clusterID, props["unit_id"])
	if err != nil {
		return err
	}
	return s.Repair(ctx, u, taskID)
}

// StopTask implements sched/runner.Runner.
func (s *Service) StopTask(ctx context.Context, clusterID, taskID uuid.UUID, props runner.TaskProperties) error {
	u, err := s.GetUnit(ctx, clusterID, props["unit_id"])
	if err != nil {
		return err
	}
	return s.StopRun(ctx, u, taskID)
}

// TaskStatus implements sched/runner.Runner.
func (s *Service) TaskStatus(ctx context.Context, clusterID, taskID uuid.UUID, props runner.TaskProperties) (runner.Status, error) {
	u, err := s.GetUnit(ctx, clusterID, props["unit_id"])
	if err != nil {
		return "", err
	}
	run, err := s.GetRun(ctx, u, taskID)
	if err != nil {
		return "", err
	}
	switch run.Status {
	case StatusRunning, StatusStopping:
		return runner.StatusRunning, nil
	case StatusError:
		return runner.StatusError, nil
	case StatusDone, StatusStopped:
		return runner.StatusStopped, nil
	default:
		return "", fmt.Errorf("unmapped repair service state %q", run.Status)
	}
}
