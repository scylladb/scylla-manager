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
	"github.com/scylladb/mermaid/dbapi"
	"github.com/scylladb/mermaid/dht"
	"github.com/scylladb/mermaid/log"
	"github.com/scylladb/mermaid/schema"
)

// globalClusterID is a special value used as a cluster ID for a global
// configuration.
var globalClusterID = mermaid.UUID{0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x40, 0x0, 0x80, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0}

// Service orchestrates cluster repairs.
type Service struct {
	session *gocql.Session
	client  dbapi.ProviderFunc
	logger  log.Logger
}

// NewService creates a new service instance.
func NewService(session *gocql.Session, p dbapi.ProviderFunc, l log.Logger) (*Service, error) {
	if session == nil || session.Closed() {
		return nil, errors.New("invalid session")
	}

	if p == nil {
		return nil, errors.New("invalid dbapi provider")
	}

	return &Service{
		session: session,
		client:  p,
		logger:  l,
	}, nil
}

// Repair starts an asynchronous repair process.
func (s *Service) Repair(ctx context.Context, u *Unit, taskID mermaid.UUID) error {
	s.logger.Debug(ctx, "Repair", "Unit", u, "TaskID", taskID)

	// validate a unit
	if err := u.Validate(); err != nil {
		return errors.Wrap(err, "invalid unit")
	}

	// get the unit configuration
	c, err := s.GetMergedUnitConfig(ctx, u)
	if err != nil {
		return errors.Wrap(err, "couldn't get a unit configuration")
	}
	s.logger.Debug(ctx, "Using config", "Config", &c.Config)

	// register a run with preparing status
	r := Run{
		ID:        taskID,
		UnitID:    u.ID,
		ClusterID: u.ClusterID,
		Status:    StatusPreparing,
		StartTime: time.Now(),
	}
	if err := s.putRun(ctx, &r); err != nil {
		errors.Wrap(err, "couldn't register the run")
	}

	// fail updates a run and passes the error
	fail := func(err error) error {
		r.Status = StatusError
		r.Cause = err.Error()

		if err := s.putRun(ctx, &r); err != nil {
			s.logger.Error(ctx, "Couldn't persist the repair failure",
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
		return fail(errors.Wrap(err, "couldn't get the cluster proxy"))
	}

	// check the cluster partitioner
	p, err := cluster.Partitioner(ctx)
	if err != nil {
		return fail(errors.Wrap(err, "couldn't get the cluster partitioner name"))
	}
	if p != dbapi.Murmur3Partitioner {
		return fail(errors.Errorf("unsupported partitioner %q, the only supported partitioner is %q", p, dbapi.Murmur3Partitioner))
	}

	// get the cluster topology hash
	tokens, err := cluster.Tokens(ctx)
	if err != nil {
		return fail(errors.Wrap(err, "couldn't get the cluster tokens"))
	}
	r.TopologyHash = topologyHash(tokens)

	// get the ring description
	_, ring, err := cluster.DescribeRing(ctx, u.Keyspace)
	if err != nil {
		return fail(errors.Wrap(err, "couldn't get the ring description"))
	}

	// get local datacenter name
	dc, err := cluster.Datacenter(ctx)
	if err != nil {
		return fail(errors.Wrap(err, "couldn't get the local datacenter name"))
	}
	s.logger.Debug(ctx, "Using DC", "dc", dc)

	// split token range into coordination hosts
	for host, segments := range hostSegments(dc, ring) {
		s.logger.Debug(ctx, "Preparing host", "Host", host)

		// split host token range to shards
		shards, err := s.shardSegments(ctx, cluster, host, segments)
		if err != nil {
			return fail(errors.Wrapf(err, "failed to prepare the repair for host %q", host))
		}

		// group shard segments
		for i, shard := range shards {
			shards[i] = splitSegments(mergeSegments(shard), *c.SegmentSizeLimit)
		}

		// register the host segments
		if err := s.putRunSegments(ctx, &r, host, shards); err != nil {
			return fail(errors.Wrapf(err, "failed to register segments for host %q", host))
		}

		// calculate shard segments' statistics
		shardStats := map[int]*stats{}
		for i, shard := range shards {
			shardStats[i] = segmentsStats(shard)
		}
		s.logger.Debug(ctx, "Prepared host",
			"Host", host,
			"Shard stats", shardStats,
		)
	}

	// run the repair
	r.Status = StatusRunning
	if err := s.putRun(ctx, &r); err != nil {
		errors.Wrap(err, "couldn't update the run status")
	}

	return nil
}

func (s *Service) shardSegments(ctx context.Context, cluster *dbapi.Client, host string, segments []*Segment) ([][]*Segment, error) {
	// get host sharding configuration
	c, err := cluster.HostConfig(ctx, host)
	if err != nil {
		return nil, errors.Wrap(err, "couldn't get host config")
	}

	var (
		shardCount, shardingIgnoreMsbBits uint
		ok                                bool
	)
	if shardCount, ok = c.ShardCount(); !ok {
		return nil, errors.Wrap(err, "config missing shard_count")
	}
	if shardingIgnoreMsbBits, ok = c.Murmur3PartitionerIgnoreMsbBits(); !ok {
		return nil, errors.Wrap(err, "config missing murmur3_partitioner_ignore_msb_bits")
	}

	// create partitioner
	partitioner := dht.NewMurmur3Partitioner(shardCount, shardingIgnoreMsbBits)

	// split segments into shards
	shards := shardSegments(segments, partitioner)

	// validate shards
	if err := validateShards(segments, shards, partitioner); err != nil {
		s.logger.Info(ctx, "Suboptimal sharding",
			"Host", host,
			"Error", err,
		)
	}

	return shards, nil
}

// putRun upserts a repair run.
func (s *Service) putRun(ctx context.Context, r *Run) error {
	s.logger.Debug(ctx, "putRun", "Run", r)

	stmt, names := schema.RepairRun.Insert()

	q := gocqlx.Query(s.session.Query(stmt).WithContext(ctx), names).BindStruct(&r)
	return q.ExecRelease()
}

// putRunSegments creates run segments for a given run and host.
func (s *Service) putRunSegments(ctx context.Context, r *Run, host string, shards [][]*Segment) error {
	rs := RunSegment{
		ClusterID:       r.ClusterID,
		UnitID:          r.UnitID,
		RunID:           r.ID,
		Status:          StatusPending,
		CoordinatorHost: host,
	}

	stmt, names := schema.RepairRunSegment.Insert()

	q := gocqlx.Query(s.session.Query(stmt).WithContext(ctx), names)
	defer q.Release()

	for shard, segments := range shards {
		rs.Shard = shard
		for _, k := range segments {
			rs.StartToken = k.StartToken
			rs.EndToken = k.EndToken
			q.BindStruct(&rs)

			if err := q.Exec(); err != nil {
				return err
			}
		}
	}

	return nil
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

// GetUnit returns repair unit based on ID. If nothing was found
// mermaid.ErrNotFound is returned.
func (s *Service) GetUnit(ctx context.Context, clusterID, ID mermaid.UUID) (*Unit, error) {
	s.logger.Debug(ctx, "GetUnit", "ClusterID", clusterID, "ID", ID)

	stmt, names := schema.RepairUnit.Get()

	q := gocqlx.Query(s.session.Query(stmt).WithContext(ctx), names).BindMap(qb.M{
		"cluster_id": clusterID,
		"id":         ID,
	})

	var u Unit
	if err := gocqlx.Get(&u, q.Query); err != nil {
		return nil, err
	}

	return &u, nil
}

// PutUnit upserts repair unit, ID is generates and set on the passed unit.
func (s *Service) PutUnit(ctx context.Context, u *Unit) error {
	s.logger.Debug(ctx, "PutUnit", "Unit", u)

	if err := u.Validate(); err != nil {
		return err
	}
	u.ID = u.genID()

	stmt, names := schema.RepairUnit.Insert()

	q := gocqlx.Query(s.session.Query(stmt).WithContext(ctx), names).BindStruct(u)

	return q.ExecRelease()
}

// DeleteUnit removes repair based on ID.
func (s *Service) DeleteUnit(ctx context.Context, clusterID, ID mermaid.UUID) error {
	s.logger.Debug(ctx, "DeleteUnit", "ClusterID", clusterID, "ID", ID)

	stmt, names := schema.RepairUnit.Delete()

	q := gocqlx.Query(s.session.Query(stmt).WithContext(ctx), names).BindMap(qb.M{
		"cluster_id": clusterID,
		"id":         ID,
	})

	return q.ExecRelease()
}
