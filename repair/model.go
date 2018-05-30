// Copyright (C) 2017 ScyllaDB

package repair

import (
	"reflect"
	"time"

	"github.com/gocql/gocql"
	"github.com/scylladb/gocqlx"
	"github.com/scylladb/mermaid/sched/runner"
	"github.com/scylladb/mermaid/uuid"
)

// Unit specifies what shall be repaired.
type Unit struct {
	Keyspace string `db:"keyspace_name"`
	Tables   []string
}

// MarshalUDT implements UDTMarshaler.
func (u Unit) MarshalUDT(name string, info gocql.TypeInfo) ([]byte, error) {
	f := gocqlx.DefaultMapper.FieldByName(reflect.ValueOf(u), name)
	return gocql.Marshal(info, f.Interface())
}

// UnmarshalUDT implements UDTUnmarshaler.
func (u *Unit) UnmarshalUDT(name string, info gocql.TypeInfo, data []byte) error {
	f := gocqlx.DefaultMapper.FieldByName(reflect.ValueOf(u), name)
	return gocql.Unmarshal(info, data, f.Addr().Interface())
}

// Run tracks repair progress, shares ID with sched.Run that initiated it.
type Run struct {
	ClusterID uuid.UUID
	TaskID    uuid.UUID
	ID        uuid.UUID

	PrevID       uuid.UUID
	TopologyHash uuid.UUID
	Units        []Unit
	Status       runner.Status
	Cause        string
	StartTime    time.Time
	EndTime      time.Time

	ClusterName string `db:"-"`
}

// RunProgress describes repair progress on per shard basis.
type RunProgress struct {
	ClusterID uuid.UUID
	TaskID    uuid.UUID
	RunID     uuid.UUID
	Unit      int
	Host      string
	Shard     int

	SegmentCount            int
	SegmentSuccess          int
	SegmentError            int
	SegmentErrorStartTokens []int64
	LastStartToken          int64
	LastStartTime           time.Time
	LastCommandID           int32
}

// complete checks if a shard is completely repaired.
func (p *RunProgress) complete() bool {
	return p.SegmentCount > 0 && p.SegmentCount == p.SegmentSuccess
}

// completeWithErrors checks if a shard tried repairing every segment.
func (p *RunProgress) completeWithErrors() bool {
	return p.SegmentCount > 0 && p.SegmentError > 0 && p.SegmentCount == p.SegmentSuccess+p.SegmentError
}

// PercentComplete returns value from 0 to 100 representing percentage of
// successfully repaired segments within a shard.
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
	return p.LastCommandID != 0 || p.SegmentSuccess > 0 || p.SegmentError > 0
}
