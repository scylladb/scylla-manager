// Copyright (C) 2017 ScyllaDB

package repair

import (
	"fmt"
	"time"

	"github.com/scylladb/mermaid/uuid"
)

// Unit specifies what shall be repaired.
type Unit struct {
	Keyspace string
	Tables   []string
}

// Segment specifies token range: [StartToken, EndToken), StartToken is always
// less then EndToken.
type Segment struct {
	StartToken int64
	EndToken   int64
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

// Run tracks repair progress, shares ID with sched.Run that initiated it.
type Run struct {
	ClusterID uuid.UUID
	TaskID    uuid.UUID
	ID        uuid.UUID

	PrevID       uuid.UUID
	TopologyHash uuid.UUID
	Keyspace     string `db:"keyspace_name"`
	Tables       []string
	Status       Status
	Cause        string
	StartTime    time.Time
	EndTime      time.Time
}

// RunProgress describes repair progress on per shard basis.
type RunProgress struct {
	ClusterID uuid.UUID
	TaskID    uuid.UUID
	RunID     uuid.UUID
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
