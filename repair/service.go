// Copyright (C) 2017 ScyllaDB

package repair

import (
	"context"
	"time"

	"github.com/gocql/gocql"
	"github.com/pkg/errors"
	"github.com/scylladb/gocqlx"
	"github.com/scylladb/gocqlx/qb"
	"github.com/scylladb/mermaid"
	"github.com/scylladb/mermaid/log"
	"github.com/scylladb/mermaid/schema"
	"github.com/scylladb/mermaid/scylla"
	"github.com/scylladb/mermaid/uuid"
)

// globalClusterID is a special value used as a cluster ID for a global
// configuration.
var globalClusterID = uuid.NewFromUint64(0, 0)

// Service orchestrates cluster repairs.
type Service struct {
	session *gocql.Session
	client  scylla.ProviderFunc
	logger  log.Logger
}

// NewService creates a new service instance.
func NewService(session *gocql.Session, p scylla.ProviderFunc, l log.Logger) (*Service, error) {
	if session == nil || session.Closed() {
		return nil, errors.New("invalid session")
	}

	if p == nil {
		return nil, errors.New("invalid scylla provider")
	}

	return &Service{
		session: session,
		client:  p,
		logger:  l,
	}, nil
}

// Repair starts an asynchronous repair process.
func (s *Service) Repair(ctx context.Context, u *Unit, taskID uuid.UUID) error {
	s.logger.Debug(ctx, "Repair", "Unit", u, "TaskID", taskID)

	// validate a unit
	if err := u.Validate(); err != nil {
		return errors.Wrap(err, "invalid unit")
	}

	// get the unit configuration
	c, err := s.GetMergedUnitConfig(ctx, u)
	if err != nil {
		return errors.Wrap(err, "failed to get a unit configuration")
	}
	s.logger.Debug(ctx, "Using config", "Config", &c.Config)

	// register a run with preparing status
	r := Run{
		ID:        taskID,
		UnitID:    u.ID,
		ClusterID: u.ClusterID,
		Keyspace:  u.Keyspace,
		Tables:    u.Tables,
		Status:    StatusRunning,
		StartTime: time.Now(),
	}
	if err := s.putRun(ctx, &r); err != nil {
		errors.Wrap(err, "failed to register the run")
	}

	// fail updates a run and passes the error
	fail := func(err error) error {
		r.Status = StatusError
		r.Cause = err.Error()

		if err := s.putRun(ctx, &r); err != nil {
			s.logger.Error(ctx, "Cannot update the run",
				"Run", &r,
				"Error", err,
			)
		}

		return err
	}

	// if repair is disabled return an error
	if !*c.Config.Enabled {
		return fail(ErrDisabled)
	}

	// get the cluster client
	cluster, err := s.client(u.ClusterID)
	if err != nil {
		return fail(errors.Wrap(err, "failed to get the cluster proxy"))
	}

	// check keyspace and tables
	all, err := cluster.Tables(ctx, r.Keyspace)
	if err != nil {
		return fail(errors.Wrap(err, "failed to get the cluster table names for keyspace"))
	}
	if len(all) == 0 {
		return fail(errors.Wrapf(err, "missing or empty keyspace %q", r.Keyspace))
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

	// get the cluster topology hash
	tokens, err := cluster.Tokens(ctx)
	if err != nil {
		return fail(errors.Wrap(err, "failed to get the cluster tokens"))
	}

	// update run with the topology hash
	r.TopologyHash = topologyHash(tokens)
	if err := s.putRun(ctx, &r); err != nil {
		return fail(errors.Wrap(err, "failed to update the run status"))
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
	hostSegments := groupSegmentsByHost(dc, ring)

	// init empty progress
	for host := range hostSegments {
		p := RunProgress{
			ClusterID: r.ClusterID,
			UnitID:    r.UnitID,
			RunID:     r.ID,
			Host:      host,
		}
		if err := s.putRunProgress(ctx, &p); err != nil {
			return fail(errors.Wrapf(err, "failed to initialise segments progress %s", &p))
		}
	}

	// spawn async repair
	wctx := log.WithTraceID(context.Background())
	s.logger.Info(ctx, "Starting async repair",
		"TaskID", taskID,
		"Unit", u,
		"WorkerTraceID", log.TraceID(wctx),
	)
	go s.asyncRepair(wctx, &r, &c.Config, cluster, hostSegments)

	return nil
}

func (s *Service) asyncRepair(ctx context.Context, r *Run, c *Config, cluster *scylla.Client, hostSegments map[string][]*Segment) {
	for host, segments := range hostSegments {
		w := worker{
			Run:      r,
			Config:   c,
			Service:  s,
			Cluster:  cluster,
			Host:     host,
			Segments: segments,

			logger: s.logger.Named("worker").With("TaskID", r.ID, "Host", host),
		}
		if err := w.exec(ctx); err != nil {
			s.logger.Error(ctx, "Worker exec failed", "Error", err)
		}
	}
}

// GetRun returns a run based on ID. If nothing was found mermaid.ErrNotFound
// is returned.
func (s *Service) GetRun(ctx context.Context, u *Unit, taskID uuid.UUID) (*Run, error) {
	s.logger.Debug(ctx, "GetRun", "Unit", u, "TaskID", taskID)

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
	s.logger.Debug(ctx, "PutRun", "Run", r)

	stmt, names := schema.RepairRun.Insert()
	q := gocqlx.Query(s.session.Query(stmt).WithContext(ctx), names).BindStruct(r)

	return q.ExecRelease()
}

// GetProgress returns run host progress. If nothing was found
// mermaid.ErrNotFound is returned.
func (s *Service) GetProgress(ctx context.Context, u *Unit, taskID uuid.UUID) ([]*RunProgress, error) {
	s.logger.Debug(ctx, "GetProgress", "Unit", u, "TaskID", taskID)

	stmt, names := schema.RepairRunProgress.Select()

	q := gocqlx.Query(s.session.Query(stmt).WithContext(ctx), names).BindMap(qb.M{
		"cluster_id": u.ClusterID,
		"unit_id":    u.ID,
		"run_id":     taskID,
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
	s.logger.Debug(ctx, "PutRunProgress", "RunProgress", p)

	stmt, names := schema.RepairRunProgress.Insert()
	q := gocqlx.Query(s.session.Query(stmt).WithContext(ctx), names).BindStruct(p)

	return q.ExecRelease()
}

// GetMergedUnitConfig returns a merged configuration for a unit.
// The configuration has no nil values. If any of the source configurations are
// disabled the resulting configuration is disabled. For other fields first
// matching configuration is used.
func (s *Service) GetMergedUnitConfig(ctx context.Context, u *Unit) (*ConfigInfo, error) {
	s.logger.Debug(ctx, "GetMergedUnitConfig", "Unit", u)

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
	s.logger.Debug(ctx, "GetConfig", "Source", src)

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
	s.logger.Debug(ctx, "PutConfig", "Source", src, "Config", c)

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
	s.logger.Debug(ctx, "DeleteConfig", "Source", src)

	stmt, names := schema.RepairConfig.Delete()
	q := gocqlx.Query(s.session.Query(stmt).WithContext(ctx), names).BindStruct(src)

	return q.ExecRelease()
}

// ListUnits returns all the Units in a given cluster.
func (s *Service) ListUnits(ctx context.Context, clusterID uuid.UUID) ([]*Unit, error) {
	s.logger.Debug(ctx, "ListUnits", "ClusterID", clusterID)

	stmt, names := schema.RepairUnit.Select()

	q := gocqlx.Query(s.session.Query(stmt).WithContext(ctx), names).BindMap(qb.M{
		"cluster_id": clusterID,
	})
	if q.Err() != nil {
		return nil, q.Err()
	}

	var units []*Unit
	if err := gocqlx.Select(&units, q.Query); err != nil {
		if err == gocql.ErrNotFound {
			return nil, nil
		}
		return nil, err
	}
	return units, nil
}

// GetUnit returns repair unit based on ID. If nothing was found
// mermaid.ErrNotFound is returned.
func (s *Service) GetUnit(ctx context.Context, clusterID, ID uuid.UUID) (*Unit, error) {
	s.logger.Debug(ctx, "GetUnit", "ClusterID", clusterID, "ID", ID)

	stmt, names := schema.RepairUnit.Get()

	q := gocqlx.Query(s.session.Query(stmt).WithContext(ctx), names).BindMap(qb.M{
		"cluster_id": clusterID,
		"id":         ID,
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

// PutUnit upserts a repair unit, unit instance must pass Validate() checks.
// If u.ID == uuid.Nil a new one is generated.
func (s *Service) PutUnit(ctx context.Context, u *Unit) error {
	s.logger.Debug(ctx, "PutUnit", "Unit", u)
	if u == nil {
		return errors.New("nil unit")
	}

	if u.ID == uuid.Nil {
		var err error
		if u.ID, err = uuid.NewRandom(); err != nil {
			return errors.Wrap(err, "couldn't generate random UUID for Unit")
		}
	}

	if err := u.Validate(); err != nil {
		return err
	}

	stmt, names := schema.RepairUnit.Insert()
	q := gocqlx.Query(s.session.Query(stmt).WithContext(ctx), names).BindStruct(u)

	return q.ExecRelease()
}

// DeleteUnit removes repair based on ID.
func (s *Service) DeleteUnit(ctx context.Context, clusterID, ID uuid.UUID) error {
	s.logger.Debug(ctx, "DeleteUnit", "ClusterID", clusterID, "ID", ID)

	stmt, names := schema.RepairUnit.Delete()

	q := gocqlx.Query(s.session.Query(stmt).WithContext(ctx), names).BindMap(qb.M{
		"cluster_id": clusterID,
		"id":         ID,
	})

	return q.ExecRelease()
}
