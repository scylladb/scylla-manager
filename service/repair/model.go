// Copyright (C) 2017 ScyllaDB

package repair

import (
	"reflect"
	"time"

	"github.com/gocql/gocql"
	"github.com/pkg/errors"
	"github.com/scylladb/gocqlx"
	"github.com/scylladb/mermaid/uuid"
)

// Unit specifies what shall be repaired.
type Unit struct {
	Keyspace      string   `json:"keyspace" db:"keyspace_name"`
	Tables        []string `json:"tables,omitempty"`
	CoordinatorDC string   `json:"-"`
	AllTables     bool     `json:"all_tables"`

	// Helper fields added initUnitWorker
	hosts  []string
	allDCs bool
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

// TokenRangesKind specifies token ranges to be repaired, DCPrimaryTokenRanges,
// PrimaryTokenRanges, NonPrimaryTokenRanges or AllTokenRanges.
type TokenRangesKind string

// TokenRangesKind enumeration
const (
	DCPrimaryTokenRanges  TokenRangesKind = "dcpr"
	PrimaryTokenRanges    TokenRangesKind = "pr"
	NonPrimaryTokenRanges TokenRangesKind = "npr"
	AllTokenRanges        TokenRangesKind = "all"
)

func (r TokenRangesKind) String() string {
	return string(r)
}

// MarshalText implements encoding.TextMarshaler.
func (r TokenRangesKind) MarshalText() (text []byte, err error) {
	return []byte(r.String()), nil
}

// UnmarshalText implements encoding.TextUnmarshaler.
func (r *TokenRangesKind) UnmarshalText(text []byte) error {
	switch TokenRangesKind(text) {
	case DCPrimaryTokenRanges:
		*r = DCPrimaryTokenRanges
	case PrimaryTokenRanges:
		*r = PrimaryTokenRanges
	case NonPrimaryTokenRanges:
		*r = NonPrimaryTokenRanges
	case AllTokenRanges:
		*r = AllTokenRanges
	default:
		return errors.Errorf("unrecognised token ranges kind %q", text)
	}
	return nil
}

// progress holds generic progress data, it's a base type for other progress
// structs.
type progress struct {
	PercentComplete float64 `json:"percent_complete"`
	PercentFailed   float64 `json:"percent_failed"`
	segmentCount    int
	segmentSuccess  int
	segmentError    int
}

func (p *progress) addProgress(n progress) {
	p.segmentCount += n.segmentCount
	p.segmentSuccess += n.segmentSuccess
	p.segmentError += n.segmentError
}

func (p *progress) calculateProgress() progress {
	if p.segmentCount == 0 {
		p.PercentComplete = 0
		p.segmentError = 0
	} else {
		p.PercentComplete = 100 * float64(p.segmentSuccess) / float64(p.segmentCount)
		p.PercentFailed = 100 * float64(p.segmentError) / float64(p.segmentCount)
	}
	return *p
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
	Units       []UnitProgress  `json:"units,omitempty"`
	DC          []string        `json:"dcs,omitempty"`
	TokenRanges TokenRangesKind `json:"token_ranges"`
}

// Target specifies what shall be repaired.
type Target struct {
	Units       []Unit          `json:"units,omitempty"`
	DC          []string        `json:"dc,omitempty"`
	Host        string          `json:"host,omitempty"`
	WithHosts   []string        `json:"with_hosts,omitempty"`
	TokenRanges TokenRangesKind `json:"token_ranges,omitempty"`
	FailFast    bool            `json:"fail_fast,omitempty"`
	Continue    bool            `json:"continue,omitempty"`
}

// Run tracks repair progress, shares ID with scheduler.Run that initiated it.
type Run struct {
	ClusterID uuid.UUID
	TaskID    uuid.UUID
	ID        uuid.UUID

	PrevID       uuid.UUID
	TopologyHash uuid.UUID
	Units        []Unit
	DC           []string
	Host         string
	WithHosts    []string
	TokenRanges  TokenRangesKind
	StartTime    time.Time

	clusterName string
	failFast    bool
	prevProg    []*RunProgress

	unitWorkers []unitWorker
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
	Keyspace    []string        `json:"keyspace"`
	DC          []string        `json:"dc"`
	Host        string          `json:"host"`
	WithHosts   []string        `json:"with_hosts"`
	TokenRanges TokenRangesKind `json:"token_ranges"`
	FailFast    bool            `json:"fail_fast"`
	Continue    bool            `json:"continue"`
}
