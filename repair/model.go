// Copyright (C) 2017 ScyllaDB

package repair

import (
	"errors"
	"sort"
	"time"

	"github.com/cespare/xxhash"
	"github.com/scylladb/mermaid"
)

// ConfigType specifies a type of configuration. Configuration object is built
// by merging configurations of different types. If configuration option is not
// found for UnitConfig then it falls back to KeyspaceConfig, ClusterConfig and
// TenantConfig.
type ConfigType string

// ConfigType enumeration.
const (
	UnitConfig     ConfigType = "unit"
	KeyspaceConfig            = "keyspace"
	ClusterConfig             = "cluster"
	tenantConfig              = "tenant"
)

// Config specifies how a Unit is repaired.
type Config struct {
	// Enabled specifies if repair should take place at all.
	Enabled *bool
	// SegmentSizeLimit specifies in how many steps a shard will be repaired,
	// increasing this value decreases singe node repair command time and
	// increases number of node repair commands.
	SegmentSizeLimit *int64
	// RetryLimit specifies how many times a failed segment should be retried
	// before reporting an error.
	RetryLimit *int
	// RetryBackoffSeconds specifies minimal time in seconds to wait before
	// retrying a failed segment.
	RetryBackoffSeconds *int
	// ParallelNodeLimit specifies how many nodes can be repaired in parallel.
	// Set to 0 for unlimited.
	ParallelNodeLimit *int
	// ParallelShardPercent specifies how many shards on a node can be repaired
	// in parallel as a percent of total shards. ParallelShardPercent takes
	// values from 0 to 1.
	ParallelShardPercent *float32
}

// Validate checks if all the fields are properly set.
func (c *Config) Validate() error {
	if c == nil {
		return errors.New("nil config")
	}

	var (
		i   int
		i64 int64
		f   float32
	)

	if c.SegmentSizeLimit != nil {
		i64 = *c.SegmentSizeLimit
		if i64 < 1 && i64 != -1 {
			return errors.New("invalid SegmentSizeLimit value, valid values are greater or equal 1")
		}
	}
	if c.RetryLimit != nil {
		i = *c.RetryLimit
		if i < 0 {
			return errors.New("invalid RetryLimit value, valid values are greater or equal 0")
		}
	}
	if c.RetryBackoffSeconds != nil {
		i = *c.RetryBackoffSeconds
		if i < 0 {
			return errors.New("invalid RetryBackoffSeconds value, valid values are greater or equal 0")
		}
	}
	if c.ParallelNodeLimit != nil {
		i = *c.ParallelNodeLimit
		if i < 1 && i != -1 {
			return errors.New("invalid ParallelNodeLimit value, valid values are greater or equal -1")
		}
	}
	if c.ParallelShardPercent != nil {
		f = *c.ParallelShardPercent
		if f < 0 || f > 1 {
			return errors.New("invalid ParallelShardPercent value, valid values are between 0 and 1")
		}
	}

	return nil
}

// ConfigSource specifies configuration target.
type ConfigSource struct {
	ClusterID  mermaid.UUID
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
	ParallelNodeLimitSource    ConfigSource
	ParallelShardPercentSource ConfigSource
}

// Unit is a set of tables in a keyspace that are repaired together.
type Unit struct {
	ID        mermaid.UUID
	ClusterID mermaid.UUID
	Keyspace  string `db:"keyspace_name"`
	Tables    []string
}

// Validate checks if all the fields are properly set.
func (u *Unit) Validate() error {
	if u == nil {
		return errors.New("nil unit")
	}
	v := mermaid.UUID{}

	if u.ClusterID == v {
		return errors.New("missing ClusterID")
	}
	if u.Keyspace == "" {
		return errors.New("missing Keyspace")
	}
	if u.ID != v && u.ID != u.genID() {
		return errors.New("invalid ID value")
	}

	return nil
}

// genID generates unit ID based on keyspace and tables.
func (u *Unit) genID() mermaid.UUID {
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

	return mermaid.UUIDFromUint64(l, r)
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
	StatusPending   Status = "pending"
	StatusPreparing Status = "preparing"
	StatusRunning   Status = "running"
	StatusSuccess   Status = "success"
	StatusError     Status = "error"
	StatusPaused    Status = "paused"
	StatusAborted   Status = "aborted"
)

// Run tracks repair progress, shares ID with sched.Run that initiated it.
type Run struct {
	ID           mermaid.UUID
	UnitID       mermaid.UUID
	ClusterID    mermaid.UUID
	TopologyHash mermaid.UUID
	Keyspace     string `db:"keyspace_name"`
	Tables       []string
	Status       Status
	Cause        string
	RestartCount int
	StartTime    time.Time
	EndTime      time.Time
	PauseTime    time.Time
}

// RunProgress describes repair progress on per shard basis.
type RunProgress struct {
	ClusterID      mermaid.UUID
	UnitID         mermaid.UUID
	RunID          mermaid.UUID
	Host           string
	Shard          int
	SegmentCount   int
	SegmentSuccess int
	SegmentError   int
	LastStartToken int64
	LastStartTime  time.Time
	LastCommandID  int32
}

// RunError holds information about run errors.
type RunError struct {
	ClusterID       mermaid.UUID
	UnitID          mermaid.UUID
	RunID           mermaid.UUID
	StartToken      int64
	EndToken        int64
	Status          Status
	Cause           string
	CoordinatorHost string
	Shard           int
	CommandID       int32
	StartTime       time.Time
	EndTime         time.Time
	FailCount       int
}
