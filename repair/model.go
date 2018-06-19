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
	Keyspace string   `db:"keyspace_name" json:"keyspace"`
	Tables   []string `json:"tables,omitempty"`

	allTables bool
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

// progress holds generic progress data, it's a base type for other progress
// structs.
type progress struct {
	PercentComplete int `json:"percent_complete"`
}

// ShardProgress specifies repair progress of a shard.
type ShardProgress struct {
	progress
	SegmentCount   int `json:"segment_count"`
	SegmentSuccess int `json:"segment_success"`
	SegmentError   int `json:"segment_error"`
}

// NodeProgress specifies repair progress of a node.
type NodeProgress struct {
	progress
	Host   string          `json:"host"`
	Shards []ShardProgress `json:"shards,omitempty"`
}

// UnitProgress specifies repair progress of a unit.
type UnitProgress struct {
	progress
	Unit  Unit           `json:"unit"`
	Nodes []NodeProgress `json:"nodes,omitempty"`
}

// Progress specifies repair progress of a run with a possibility to dig down
// units, nodes and shards.
type Progress struct {
	progress
	Units []UnitProgress `json:"units,omitempty"`
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

	clusterName string
	prevProg    []*RunProgress
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

// taskProperties is the main data structure of the runner.Properties blob.
type taskProperties struct {
	Filter []string `json:"filter"`
}
