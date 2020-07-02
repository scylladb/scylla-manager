// Copyright (C) 2017 ScyllaDB

package repair

import (
	"sort"
	"time"

	"github.com/scylladb/go-set/iset"
	"github.com/scylladb/mermaid/pkg/util/inexlist/ksfilter"
	"github.com/scylladb/mermaid/pkg/util/uuid"
)

// Unit represents keyspace and its tables.
type Unit = ksfilter.Unit

// Target specifies what shall be repaired.
type Target struct {
	Units     []Unit   `json:"units"`
	DC        []string `json:"dc"`
	FailFast  bool     `json:"fail_fast"`
	Continue  bool     `json:"continue"`
	Intensity float64  `json:"intensity"`
}

// taskProperties is the main data structure of the runner.Properties blob.
type taskProperties struct {
	Keyspace  []string `json:"keyspace"`
	DC        []string `json:"dc"`
	FailFast  bool     `json:"fail_fast"`
	Continue  bool     `json:"continue"`
	Intensity float64  `json:"intensity"`
}

func defaultTaskProperties() *taskProperties {
	return &taskProperties{
		Continue: true,
	}
}

// Run tracks repair, shares ID with scheduler.Run that initiated it.
type Run struct {
	ClusterID uuid.UUID
	TaskID    uuid.UUID
	ID        uuid.UUID

	DC        []string
	PrevID    uuid.UUID
	StartTime time.Time

	clusterName string
}

// RunProgress specifies repair progress of a run for a table.
type RunProgress struct {
	ClusterID uuid.UUID
	TaskID    uuid.UUID
	RunID     uuid.UUID
	Host      string
	Keyspace  string `db:"keyspace_name"`
	Table     string `db:"table_name"`

	TokenRanges int64
	Success     int64
	Error       int64
	StartedAt   *time.Time
	CompletedAt *time.Time
}

// RunState represents state of the repair.
// Used for resuming repair task from the last known state.
type RunState struct {
	ClusterID uuid.UUID
	TaskID    uuid.UUID
	RunID     uuid.UUID
	Keyspace  string `db:"keyspace_name"`
	Table     string `db:"table_name"`

	SuccessPos []int
	ErrorPos   []int
}

// UpdatePositions updates SuccessPos and ErrorPos according to job result.
func (rs *RunState) UpdatePositions(job jobResult) {
	errorPos := iset.New(rs.ErrorPos...)
	successPos := iset.New(rs.SuccessPos...)
	for _, tr := range job.Ranges {
		if job.Err != nil {
			if tr.Keyspace == rs.Keyspace && tr.Table == rs.Table {
				errorPos.Add(tr.Pos)
			}
		} else {
			if tr.Keyspace == rs.Keyspace && tr.Table == rs.Table {
				successPos.Add(tr.Pos)
			}
		}
	}
	rs.ErrorPos = errorPos.List()
	sort.Ints(rs.ErrorPos)
	rs.SuccessPos = successPos.List()
	sort.Ints(rs.SuccessPos)
}

// progress holds generic progress data, it's a base type for other progress
// structs.
type progress struct {
	TokenRanges int64      `json:"token_ranges"`
	Success     int64      `json:"success"`
	Error       int64      `json:"error"`
	StartedAt   *time.Time `json:"started_at"`
	CompletedAt *time.Time `json:"completed_at"`
}

// HostProgress specifies repair progress of a host.
type HostProgress struct {
	progress
	Host      string          `json:"host"`
	Tables    []TableProgress `json:"tables,omitempty"`
	Intensity float64         `json:"intensity"`
}

// TableProgress represents progress for table for all all hosts.
type TableProgress struct {
	progress
	Keyspace string `json:"keyspace"`
	Table    string `json:"table"`
}

// Progress breakdown repair progress by tables for all hosts and each host
// separately.
type Progress struct {
	progress
	DC     []string        `json:"dcs"`
	Hosts  []HostProgress  `json:"hosts"`
	Tables []TableProgress `json:"tables"`
}
