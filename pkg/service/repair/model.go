// Copyright (C) 2017 ScyllaDB

package repair

import (
	"sort"
	"time"

	"github.com/scylladb/go-set/iset"
	"github.com/scylladb/scylla-manager/pkg/util/inexlist/ksfilter"
	"github.com/scylladb/scylla-manager/pkg/util/uuid"
)

// Unit represents keyspace and its tables.
type Unit = ksfilter.Unit

// Target specifies what shall be repaired.
type Target struct {
	Units               []Unit   `json:"units"`
	DC                  []string `json:"dc"`
	Host                string   `json:"host,omitempty"`
	FailFast            bool     `json:"fail_fast"`
	Continue            bool     `json:"continue"`
	Intensity           float64  `json:"intensity"`
	Parallel            int      `json:"parallel"`
	SmallTableThreshold int64    `json:"small_table_threshold"`
}

// taskProperties is the main data structure of the runner.Properties blob.
type taskProperties struct {
	Keyspace            []string `json:"keyspace"`
	DC                  []string `json:"dc"`
	Host                string   `json:"host"`
	FailFast            bool     `json:"fail_fast"`
	Continue            bool     `json:"continue"`
	Intensity           float64  `json:"intensity"`
	Parallel            int      `json:"parallel"`
	SmallTableThreshold int64    `json:"small_table_threshold"`
}

func defaultTaskProperties() *taskProperties {
	return &taskProperties{
		Continue:  true,
		Intensity: 1,

		// Consider 1GB table as small by default.
		SmallTableThreshold: 1 * 1024 * 1024 * 1024,
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

	StartedAt         *time.Time
	CompletedAt       *time.Time
	Duration          time.Duration
	DurationStartedAt *time.Time
	runningJobCount   int
}

// Completed returns true if all token rages are processed.
func (rp *RunProgress) Completed() bool {
	return rp.TokenRanges == rp.Success+rp.Error
}

// CurrentDuration returns duration which includes elapsed time for uncompleted
// jobs.
// now parameter is used as reference point.
func (rp *RunProgress) CurrentDuration(now time.Time) time.Duration {
	// Checking if run progress is started (StartedAt) and if a job is started
	// (DurationStartedAt).
	if rp.StartedAt != nil && rp.DurationStartedAt != nil {
		return rp.Duration + now.Sub(*rp.DurationStartedAt)
	}

	return rp.Duration
}

// AddDuration resets running jobs timer and sums up running duration stats.
func (rp *RunProgress) AddDuration(end time.Time) {
	rp.Duration += end.Sub(*rp.DurationStartedAt)
	rp.DurationStartedAt = nil
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
	Duration    int64      `json:"duration_ms"`
}

// ProgressPercentage returns repair progress percentage based on token ranges.
func (p progress) PercentComplete() int {
	if p.TokenRanges == 0 {
		return 0
	}

	if p.Success >= p.TokenRanges {
		return 100
	}

	percent := 100 * p.Success / p.TokenRanges
	if percent >= 100 {
		percent = 99
	}

	return int(percent)
}

// HostProgress specifies repair progress of a host.
type HostProgress struct {
	progress
	Host   string          `json:"host"`
	Tables []TableProgress `json:"tables,omitempty"`
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
	DC        []string        `json:"dcs"`
	Hosts     []HostProgress  `json:"hosts"`
	Tables    []TableProgress `json:"tables"`
	Intensity float64         `json:"intensity"`
	Parallel  int             `json:"parallel"`
}
