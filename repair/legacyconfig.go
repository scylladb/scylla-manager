// Copyright (C) 2017 ScyllaDB

package repair

import (
	"context"
	"fmt"

	"github.com/pkg/errors"
	"github.com/scylladb/gocqlx"
	"github.com/scylladb/gocqlx/qb"
	"github.com/scylladb/mermaid"
	"github.com/scylladb/mermaid/schema"
	"github.com/scylladb/mermaid/uuid"
	"go.uber.org/multierr"
)

// globalClusterID is a special value used as a cluster ID for a global
// configuration.
var globalClusterID = uuid.NewFromUint64(0, 0)

// LegacyConfigType specifies a type of configuration. Configuration object is built
// by merging configurations of different types. If configuration option is not
// found for UnitConfig then it falls back to KeyspaceConfig, ClusterConfig and
// TenantConfig.
type LegacyConfigType string

// LegacyConfigType enumeration.
const (
	UnknownConfigType LegacyConfigType = "unknown"
	UnitConfig        LegacyConfigType = "unit"
	KeyspaceConfig    LegacyConfigType = "keyspace"
	ClusterConfig     LegacyConfigType = "cluster"
	tenantConfig      LegacyConfigType = "tenant"
)

func (c LegacyConfigType) String() string {
	return string(c)
}

// MarshalText implements encoding.TextMarshaler.
func (c LegacyConfigType) MarshalText() (text []byte, err error) {
	return []byte(c.String()), nil
}

// UnmarshalText implements encoding.TextUnmarshaler.
func (c *LegacyConfigType) UnmarshalText(text []byte) error {
	switch LegacyConfigType(text) {
	case UnknownConfigType:
		*c = UnknownConfigType
	case UnitConfig:
		*c = UnitConfig
	case KeyspaceConfig:
		*c = KeyspaceConfig
	case ClusterConfig:
		*c = ClusterConfig
	case tenantConfig:
		*c = tenantConfig
	default:
		return fmt.Errorf("unrecognized LegacyConfigType %q", text)
	}
	return nil
}

// LegacyConfig specifies how a Unit is repaired.
type LegacyConfig struct {
	// Enabled specifies if repair should take place at all.
	Enabled *bool `json:"enabled,omitempty"`
	// SegmentSizeLimit specifies in how many steps a shard will be repaired,
	// increasing this value decreases singe node repair command time and
	// increases number of node repair commands.
	SegmentSizeLimit *int64 `json:"segment_size_limit,omitempty"`
	// RetryLimit specifies how many times a failed segment should be retried
	// before reporting an error.
	RetryLimit *int `json:"retry_limit,omitempty"`
	// RetryBackoffSeconds specifies minimal time in seconds to wait before
	// retrying a failed segment.
	RetryBackoffSeconds *int `json:"retry_backoff_seconds,omitempty"`
	// ParallelShardPercent specifies how many shards on a node can be repaired
	// in parallel as a percent of total shards. ParallelShardPercent takes
	// values from 0 to 1.
	ParallelShardPercent *float32 `json:"parallel_shard_percent,omitempty"`
}

// Validate checks if all the fields are properly set.
func (c *LegacyConfig) Validate() (err error) {
	if c == nil {
		return mermaid.ErrNilPtr
	}

	var (
		i   int
		i64 int64
		f   float32
	)

	if c.SegmentSizeLimit != nil {
		i64 = *c.SegmentSizeLimit
		if i64 < 1 && i64 != -1 {
			err = multierr.Append(err, errors.New("invalid SegmentSizeLimit value, valid values are greater or equal 1"))
		}
	}
	if c.RetryLimit != nil {
		i = *c.RetryLimit
		if i < 0 {
			err = multierr.Append(err, errors.New("invalid RetryLimit value, valid values are greater or equal 0"))
		}
	}
	if c.RetryBackoffSeconds != nil {
		i = *c.RetryBackoffSeconds
		if i < 0 {
			err = multierr.Append(err, errors.New("invalid RetryBackoffSeconds value, valid values are greater or equal 0"))
		}
	}
	if c.ParallelShardPercent != nil {
		f = *c.ParallelShardPercent
		if f < 0 || f > 1 {
			err = multierr.Append(err, errors.New("invalid ParallelShardPercent value, valid values are between 0 and 1"))
		}
	}

	return
}

// LegacyConfigSource specifies configuration target.
type LegacyConfigSource struct {
	ClusterID  uuid.UUID
	Type       LegacyConfigType
	ExternalID string
}

// LegacyConfigInfo is configuration together with source info.
type LegacyConfigInfo struct {
	LegacyConfig

	EnabledSource              LegacyConfigSource
	SegmentSizeLimitSource     LegacyConfigSource
	RetryLimitSource           LegacyConfigSource
	RetryBackoffSecondsSource  LegacyConfigSource
	ParallelShardPercentSource LegacyConfigSource
}

// GetMergedUnitConfig returns a merged configuration for a unit.
// The configuration has no nil values. If any of the source configurations are
// disabled the resulting configuration is disabled. For other fields first
// matching configuration is used.
func (s *Service) GetMergedUnitConfig(ctx context.Context, u *Unit) (*LegacyConfigInfo, error) {
	s.logger.Debug(ctx, "GetMergedUnitConfig", "unit", u)

	// validate the unit
	if err := u.Validate(); err != nil {
		return nil, mermaid.ParamError{Cause: errors.Wrap(err, "invalid unit")}
	}

	order := []LegacyConfigSource{
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

	all := make([]*LegacyConfig, 0, len(order))
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

// mergeConfigs does the configuration merging for Service.GetMergedUnitConfig.
func mergeConfigs(all []*LegacyConfig, src []LegacyConfigSource) (*LegacyConfigInfo, error) {
	if len(all) == 0 {
		return nil, errors.New("no matching configurations")
	}

	m := LegacyConfigInfo{}

	// Enabled *bool
	for i, c := range all {
		if c.Enabled != nil {
			if m.Enabled == nil || !*c.Enabled {
				m.Enabled = c.Enabled
				m.EnabledSource = src[i]
			}
		}
	}
	if m.Enabled == nil {
		return nil, errors.New("no value for Enabled")
	}

	// SegmentSizeLimit *int64
	for i, c := range all {
		if c.SegmentSizeLimit != nil {
			m.SegmentSizeLimit = c.SegmentSizeLimit
			m.SegmentSizeLimitSource = src[i]
			break
		}
	}
	if m.SegmentSizeLimit == nil {
		return nil, errors.New("no value for SegmentSizeLimit")
	}

	// RetryLimit *int
	for i, c := range all {
		if c.RetryLimit != nil {
			m.RetryLimit = c.RetryLimit
			m.RetryLimitSource = src[i]
			break
		}
	}
	if m.RetryLimit == nil {
		return nil, errors.New("no value for RetryLimit")
	}

	// RetryBackoffSeconds *int
	for i, c := range all {
		if c.RetryBackoffSeconds != nil {
			m.RetryBackoffSeconds = c.RetryBackoffSeconds
			m.RetryBackoffSecondsSource = src[i]
			break
		}
	}
	if m.RetryBackoffSeconds == nil {
		return nil, errors.New("no value for RetryBackoffSeconds")
	}

	// ParallelShardPercent *float32
	for i, c := range all {
		if c.ParallelShardPercent != nil {
			m.ParallelShardPercent = c.ParallelShardPercent
			m.ParallelShardPercentSource = src[i]
			break
		}
	}
	if m.ParallelShardPercent == nil {
		return nil, errors.New("no value for ParallelShardPercent")
	}

	return &m, nil
}

// GetConfig returns repair configuration for a given object. If nothing was
// found mermaid.ErrNotFound is returned.
func (s *Service) GetConfig(ctx context.Context, src LegacyConfigSource) (*LegacyConfig, error) {
	s.logger.Debug(ctx, "GetConfig", "source", src)

	stmt, names := schema.RepairConfig.Get()

	q := gocqlx.Query(s.session.Query(stmt).WithContext(ctx), names).BindStruct(src)
	if q.Err() != nil {
		return nil, q.Err()
	}

	var c LegacyConfig
	if err := gocqlx.Iter(q.Query).Unsafe().Get(&c); err != nil {
		return nil, err
	}

	return &c, nil
}

// PutConfig upserts repair configuration for a given object.
func (s *Service) PutConfig(ctx context.Context, src LegacyConfigSource, c *LegacyConfig) error {
	s.logger.Debug(ctx, "PutConfig", "source", src, "config", c)

	if err := c.Validate(); err != nil {
		return mermaid.ParamError{Cause: errors.Wrap(err, "invalid config")}
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
func (s *Service) DeleteConfig(ctx context.Context, src LegacyConfigSource) error {
	s.logger.Debug(ctx, "DeleteConfig", "source", src)

	stmt, names := schema.RepairConfig.Delete()
	q := gocqlx.Query(s.session.Query(stmt).WithContext(ctx), names).BindStruct(src)

	return q.ExecRelease()
}
