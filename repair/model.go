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

// Unit is a set of tables in a keyspace that are repaired together.
type Unit struct {
	ID        uuid.UUID `json:"id"`
	ClusterID uuid.UUID `json:"cluster_id"`
	Name      string    `json:"name,omitempty"`
	Keyspace  string    `db:"keyspace_name" json:"keyspace"`
	Tables    []string  `json:"tables"`
}

// String returns unit Name or ID if Name is is empty.
func (u *Unit) String() string {
	if u.Name != "" {
		return u.Name
	}
	return u.ID.String()
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
