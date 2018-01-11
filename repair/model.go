// Copyright (C) 2017 ScyllaDB

package repair

import (
	"errors"
	"fmt"
	"sort"
	"time"

	"github.com/cespare/xxhash"
	"github.com/scylladb/mermaid"
	"github.com/scylladb/mermaid/uuid"
	"go.uber.org/multierr"
)

// ConfigType specifies a type of configuration. Configuration object is built
// by merging configurations of different types. If configuration option is not
// found for UnitConfig then it falls back to KeyspaceConfig, ClusterConfig and
// TenantConfig.
type ConfigType string

// ConfigType enumeration.
const (
	UnknownConfigType ConfigType = "unknown"
	UnitConfig        ConfigType = "unit"
	KeyspaceConfig    ConfigType = "keyspace"
	ClusterConfig     ConfigType = "cluster"
	tenantConfig      ConfigType = "tenant"
)

func (c ConfigType) String() string {
	return string(c)
}

// MarshalText implements encoding.TextMarshaler.
func (c ConfigType) MarshalText() (text []byte, err error) {
	return []byte(c.String()), nil
}

// UnmarshalText implements encoding.TextUnmarshaler.
func (c *ConfigType) UnmarshalText(text []byte) error {
	switch ConfigType(text) {
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
		return fmt.Errorf("unrecognized ConfigType %q", text)
	}
	return nil
}

// Config specifies how a Unit is repaired.
type Config struct {
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
func (c *Config) Validate() (err error) {
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

// ConfigSource specifies configuration target.
type ConfigSource struct {
	ClusterID  uuid.UUID
	Type       ConfigType
	ExternalID string
}

// ConfigInfo is configuration together with source info.
type ConfigInfo struct {
	Config

	EnabledSource              ConfigSource
	SegmentSizeLimitSource     ConfigSource
	RetryLimitSource           ConfigSource
	RetryBackoffSecondsSource  ConfigSource
	ParallelShardPercentSource ConfigSource
}

// Unit is a set of tables in a keyspace that are repaired together.
type Unit struct {
	ID        uuid.UUID `json:"id"`
	ClusterID uuid.UUID `json:"cluster_id"`
	Name      string    `json:"name,omitempty"`
	Keyspace  string    `db:"keyspace_name" json:"keyspace"`
	Tables    []string  `json:"tables"`
}

// Validate checks if all the fields are properly set.
func (u *Unit) Validate() (err error) {
	if u == nil {
		return mermaid.ErrNilPtr
	}

	if u.ID == uuid.Nil {
		err = multierr.Append(err, errors.New("missing ID"))
	}
	if u.ClusterID == uuid.Nil {
		err = multierr.Append(err, errors.New("missing ClusterID"))
	}
	if _, e := uuid.Parse(u.Name); e == nil {
		err = multierr.Append(err, errors.New("name cannot be an UUID"))
	}
	if u.Keyspace == "" {
		err = multierr.Append(err, errors.New("missing Keyspace"))
	}

	return
}

// UnitFilter filters units.
type UnitFilter struct {
	Name string
}

// Validate checks if all the fields are properly set.
func (f *UnitFilter) Validate() (err error) {
	if f == nil {
		return mermaid.ErrNilPtr
	}

	if _, e := uuid.Parse(f.Name); e == nil {
		err = multierr.Append(err, errors.New("name cannot be an UUID"))
	}

	return
}

// genID generates unit ID based on keyspace and tables.
func (u *Unit) genID() uuid.UUID {
	xx := xxhash.New()
	xx.Write([]byte(u.Keyspace))
	l := xx.Sum64()
	xx.Reset()

	// sort
	sort.Strings(u.Tables)
	// skip duplicates
	for i, t := range u.Tables {
		if i == 0 || u.Tables[i-1] != t {
			xx.Write([]byte(t))
		}
	}
	r := xx.Sum64()

	return uuid.NewFromUint64(l, r)
}

// Segment specifies token range: [StartToken, EndToken), StartToken is always
// less then EndToken.
type Segment struct {
	StartToken int64
	EndToken   int64
}

// stats holds segments statistics.
type stats struct {
	Size        int
	MaxRange    int64
	AvgRange    int64
	AvgMaxRatio float64
}

// Status specifies the status of a Run.
type Status string

// Status enumeration.
const (
	StatusRunning  Status = "running"
	StatusDone     Status = "done"
	StatusError    Status = "error"
	StatusStopping Status = "stopping"
	StatusStopped  Status = "stopped"
)

func (s Status) String() string {
	return string(s)
}

// MarshalText implements encoding.TextMarshaler.
func (s Status) MarshalText() (text []byte, err error) {
	return []byte(s.String()), nil
}

// UnmarshalText implements encoding.TextUnmarshaler.
func (s *Status) UnmarshalText(text []byte) error {
	switch Status(text) {
	case StatusRunning:
		*s = StatusRunning
	case StatusDone:
		*s = StatusDone
	case StatusError:
		*s = StatusError
	case StatusStopping:
		*s = StatusStopping
	case StatusStopped:
		*s = StatusStopped
	default:
		return fmt.Errorf("unrecognized Status %q", text)
	}
	return nil
}

// RunFilter filters runs.
type RunFilter struct {
	Limit uint
}

// Validate checks if all the fields are properly set.
func (f *RunFilter) Validate() error {
	if f == nil {
		return mermaid.ErrNilPtr
	}

	return nil
}

// Run tracks repair progress, shares ID with sched.Run that initiated it.
type Run struct {
	ClusterID    uuid.UUID
	UnitID       uuid.UUID
	ID           uuid.UUID
	PrevID       uuid.UUID
	TopologyHash uuid.UUID
	Keyspace     string `db:"keyspace_name"`
	Tables       []string
	Status       Status
	Cause        string
	RestartCount int
	StartTime    time.Time
	EndTime      time.Time
}

// RunProgress describes repair progress on per shard basis.
type RunProgress struct {
	ClusterID               uuid.UUID
	UnitID                  uuid.UUID
	RunID                   uuid.UUID
	Host                    string
	Shard                   int
	SegmentCount            int
	SegmentSuccess          int
	SegmentError            int
	SegmentErrorStartTokens []int64
	LastStartToken          int64
	LastStartTime           time.Time
	LastCommandID           int32
}

// Done returns true if all the segments were processed.
func (p *RunProgress) Done() bool {
	return p.SegmentCount > 0 && p.SegmentCount == p.SegmentSuccess+p.SegmentError
}

// PercentComplete returns value from 0 to 100 representing percentage of
// successfully processed segments within a shard.
func (p *RunProgress) PercentComplete() int {
	if p.SegmentCount == 0 {
		return 0
	}

	if p.SegmentSuccess >= p.SegmentCount {
		return 100
	}

	percent := 100 * p.SegmentSuccess / p.SegmentCount
	if percent >= 100 {
		percent = 99
	}

	return percent
}

// started returns true if the host / shard was ever repaired in the run.
func (p *RunProgress) started() bool {
	return !p.LastStartTime.IsZero()
}
