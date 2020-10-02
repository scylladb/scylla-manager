// Copyright (C) 2017 ScyllaDB

package repair

import (
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/gocql/gocql"
	"github.com/pkg/errors"
	"github.com/scylladb/go-set/iset"
	"github.com/scylladb/mermaid/pkg/util/inexlist/ksfilter"
	"github.com/scylladb/mermaid/pkg/util/timeutc"
	"github.com/scylladb/mermaid/pkg/util/uuid"
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

	clusterName string
}

type interval struct {
	Start time.Time
	End   *time.Time
}

type intervalSlice []interval

func (in intervalSlice) Len() int      { return len(in) }
func (in intervalSlice) Swap(i, j int) { in[i], in[j] = in[j], in[i] }
func (in intervalSlice) Less(i, j int) bool {
	return in[i].Start.Before(in[j].Start)
}

func (in intervalSlice) MinStart() *time.Time {
	if len(in) == 0 {
		return nil
	}
	return &in[0].Start
}

func (in intervalSlice) MaxEnd() *time.Time {
	if len(in) == 0 {
		return nil
	}
	var end time.Time
	for i := range in {
		if in[i].End == nil {
			return nil
		}
		if in[i].End.After(end) {
			end = *in[i].End
		}
	}

	return &end
}

// Duration calculates total duration for all intervals while ignoring
// overlapped intervals and gaps between them.
// If maxEnd is provided it will be used as ending time for incomplete
// intervals.
// If maxEnd is not provided timeutc.Now will be used.
// intervalSlice must be sorted by Start ASC.
func (in intervalSlice) Duration(maxEnd *time.Time) time.Duration {
	if len(in) == 0 {
		return 0
	}

	var (
		total time.Duration
	)

	if maxEnd == nil {
		n := timeutc.Now()
		maxEnd = &n
	}

	// Loop through intervals sorted by start time while selecting a pivot
	// which will consume all the overlapping intervals.
	// Pivot interval is enlarged if next encountered interval is longer, or
	// it is included in total duration if the next encountered interval is
	// starting later than the pivot interval ends which signals gap in
	// execution.
	var (
		pivot = in[0]
	)
	for i := 1; i <= len(in); i++ {
		if pivot.End == nil {
			pivot.End = maxEnd
		}
		if maxEnd.Equal(*pivot.End) || i == len(in) {
			total += pivot.End.Sub(pivot.Start)
			break
		}
		cur := in[i]
		if cur.End == nil {
			cur.End = maxEnd
		}
		if pivot.End.Before(cur.Start) {
			total += pivot.End.Sub(pivot.Start)
			pivot = cur
			continue
		}
		if pivot.End.Before(*cur.End) {
			pivot.End = cur.End
		}
	}

	return total
}

// JobIDTuple combines node and id of the job that was executed on that node.
type JobIDTuple struct {
	Master string
	ID     int32
}

// MarshalCQL implements gocql.Marshaler.
func (id JobIDTuple) MarshalCQL(info gocql.TypeInfo) ([]byte, error) {
	switch info.Type() {
	case gocql.TypeVarchar, gocql.TypeText:
	default:
		return nil, errors.Errorf("unsupported type %q", info.Type())
	}

	return []byte(fmt.Sprintf("%s,%d", id.Master, id.ID)), nil
}

// UnmarshalCQL implements gocql.Unmarshaler.
func (id *JobIDTuple) UnmarshalCQL(info gocql.TypeInfo, data []byte) error {
	switch info.Type() {
	case gocql.TypeVarchar, gocql.TypeText:
	default:
		return errors.Errorf("unsupported type %q", info.Type())
	}

	if len(data) == 0 {
		return nil
	}

	vals := strings.Split(string(data), ",")
	if len(vals) != 2 {
		return errors.Errorf("invalid job_id format: %q", data)
	}
	i, err := strconv.Atoi(vals[1])
	if err != nil {
		return err
	}

	*id = JobIDTuple{Master: vals[0], ID: int32(i)}
	return nil
}

// JobExecution tracks job execution info like start and end time.
type JobExecution struct {
	interval

	ClusterID uuid.UUID
	TaskID    uuid.UUID
	RunID     uuid.UUID
	Keyspace  string `db:"keyspace_name"`
	Table     string `db:"table_name"`
	Host      string
	JobID     JobIDTuple
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
