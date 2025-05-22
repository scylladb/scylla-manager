// Copyright (C) 2017 ScyllaDB

package repair

import (
	"database/sql"
	"net/netip"
	"time"

	"github.com/scylladb/scylla-manager/v3/pkg/scyllaclient"
	"github.com/scylladb/scylla-manager/v3/pkg/util/inexlist/ksfilter"
	"github.com/scylladb/scylla-manager/v3/pkg/util/uuid"
	"github.com/scylladb/scylla-manager/v3/sqlc/queries"
)

// Unit represents keyspace and its tables.
type Unit = ksfilter.Unit

// Intensity represents parsed internal intensity.
type Intensity int

// NewIntensity returns Intensity.
func NewIntensity(i int) Intensity {
	return Intensity(i)
}

// NewIntensityFromDeprecated returns Intensity parsed from deprecated float value.
func NewIntensityFromDeprecated(i float64) Intensity {
	if 0 < i && i < 1 {
		return defaultIntensity
	}
	return Intensity(i)
}

// Target specifies what shall be repaired.
type Target struct {
	Units []Unit     `json:"units"`
	DC    []string   `json:"dc"`
	Host  netip.Addr `json:"host,omitempty"`
	// Down hosts excluded from repair by the --ignore-down-hosts flag.
	IgnoreHosts         []netip.Addr `json:"ignore_hosts,omitempty"`
	FailFast            bool         `json:"fail_fast"`
	Continue            bool         `json:"continue"`
	Intensity           Intensity    `json:"intensity"`
	Parallel            int          `json:"parallel"`
	SmallTableThreshold int64        `json:"small_table_threshold"`
}

// taskProperties is the main data structure of the runner.Properties blob.
type taskProperties struct {
	Keyspace            []string `json:"keyspace"`
	DC                  []string `json:"dc"`
	Host                string   `json:"host"`
	IgnoreDownHosts     bool     `json:"ignore_down_hosts"`
	FailFast            bool     `json:"fail_fast"`
	Continue            bool     `json:"continue"`
	Intensity           float64  `json:"intensity"`
	Parallel            int      `json:"parallel"`
	SmallTableThreshold int64    `json:"small_table_threshold"`
}

func defaultTaskProperties() *taskProperties {
	return &taskProperties{
		// Don't repair system_traces unless it has been deliberately specified.
		Keyspace: []string{"*", "!system_traces"},

		Continue:  true,
		Intensity: float64(defaultIntensity),

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
	Host      string
	Parallel  int
	Intensity Intensity
	PrevID    uuid.UUID
	StartTime time.Time
	EndTime   time.Time
}

// RunProgress specifies repair progress of a run for a table.
type RunProgress struct {
	ClusterID uuid.UUID
	TaskID    uuid.UUID
	RunID     uuid.UUID
	Host      string
	Keyspace  string `db:"keyspace_name"`
	Table     string `db:"table_name"`

	Size        int64
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

// CurrentDuration returns duration which includes elapsed time for uncompleted jobs.
// now parameter is used as reference point.
func (rp *RunProgress) CurrentDuration(now time.Time) time.Duration {
	if isTimeSet(rp.StartedAt) && isTimeSet(rp.DurationStartedAt) {
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
	ClusterID     uuid.UUID
	TaskID        uuid.UUID
	RunID         uuid.UUID
	Keyspace      string                    `db:"keyspace_name"`
	Table         string                    `db:"table_name"`
	SuccessRanges []scyllaclient.TokenRange `db:"success_ranges"`
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

// PercentComplete returns repair progress percentage based on token ranges.
func (p progress) PercentComplete() int {
	if p.TokenRanges == 0 {
		return 0
	}
	percent := 100 * p.Success / p.TokenRanges
	if percent > 100 {
		percent = 100
	}
	return int(percent)
}

// HostProgress specifies repair progress of a host.
type HostProgress struct {
	progress
	Host   string          `json:"host"`
	Tables []TableProgress `json:"tables,omitempty"`
}

// TableProgress represents progress for table for all hosts.
type TableProgress struct {
	progress
	Keyspace string `json:"keyspace"`
	Table    string `json:"table"`
}

// Progress breakdown repair progress by tables for all hosts and each host
// separately.
type Progress struct {
	progress
	SuccessPercentage int             `json:"success_percentage"`
	ErrorPercentage   int             `json:"error_percentage"`
	DC                []string        `json:"dcs"`
	Host              string          `json:"host"`
	Hosts             []HostProgress  `json:"hosts"`
	Tables            []TableProgress `json:"tables"`
	MaxIntensity      Intensity       `json:"max_intensity"`
	Intensity         Intensity       `json:"intensity"`
	MaxParallel       int             `json:"max_parallel"`
	Parallel          int             `json:"parallel"`
}

func isTimeSet(t *time.Time) bool {
	return t != nil && !t.IsZero()
}

func sqlToGocqlRun(run queries.RepairRun) *Run {
	return &Run{
		ClusterID: uuid.MustParse(run.ClusterID),
		TaskID:    uuid.MustParse(run.TaskID),
		ID:        uuid.MustParse(run.ID),
		Host:      run.Host,
		DC:        []string{"dc1"},
		Parallel:  int(run.Parallel.Int64),
		Intensity: Intensity(run.Intensity.Int64),
		PrevID:    uuid.MustParse(run.PrevID),
		StartTime: run.StartTime,
		EndTime:   run.EndTime,
	}
}

func sqlToGocqlRunProgress(run queries.RepairRunProgress) *RunProgress {
	var startedAt, completedAt, durationStartedAt *time.Time
	if run.StartedAt.Valid {
		startedAt = &run.StartedAt.Time
	}
	if run.CompletedAt.Valid {
		completedAt = &run.CompletedAt.Time
	}
	if run.DurationStartedAt.Valid {
		durationStartedAt = &run.DurationStartedAt.Time
	}
	return &RunProgress{
		ClusterID:         uuid.MustParse(run.ClusterID),
		TaskID:            uuid.MustParse(run.TaskID),
		RunID:             uuid.MustParse(run.RunID),
		Host:              run.Host,
		Keyspace:          run.KeyspaceName,
		Table:             run.TableName,
		Size:              run.Size,
		TokenRanges:       run.TokenRanges,
		Success:           run.Success,
		Error:             run.Error,
		StartedAt:         startedAt,
		CompletedAt:       completedAt,
		Duration:          time.Duration(run.Duration),
		DurationStartedAt: durationStartedAt,
	}
}

func sqlToGocqlRunState(run queries.RepairRunState) *RunState {
	return &RunState{
		ClusterID:     uuid.MustParse(run.ClusterID),
		TaskID:        uuid.MustParse(run.TaskID),
		RunID:         uuid.MustParse(run.RunID),
		Keyspace:      run.KeyspaceName,
		Table:         run.TableName,
		SuccessRanges: []scyllaclient.TokenRange{{1, 2}},
	}
}

func gocqlToSqlRun(run Run) queries.RepairRun {
	return queries.RepairRun{
		ClusterID: run.ClusterID.String(),
		TaskID:    run.TaskID.String(),
		ID:        run.ID.String(),
		Dc:        []byte{48, 49, 50, 51, 52, 53, 54, 55, 56, 57},
		EndTime:   run.EndTime,
		Host:      run.Host,
		PrevID:    run.PrevID.String(),
		StartTime: run.StartTime,
	}
}

func gocqlToSqlRunProgress(run RunProgress) queries.RepairRunProgress {
	var completedAt, startedAt, durationStartedAt sql.NullTime
	if run.CompletedAt != nil {
		completedAt = sql.NullTime{Time: *run.CompletedAt, Valid: true}
	}
	if run.StartedAt != nil {
		startedAt = sql.NullTime{Time: *run.StartedAt, Valid: true}
	}
	if run.DurationStartedAt != nil {
		durationStartedAt = sql.NullTime{Time: *run.DurationStartedAt, Valid: true}
	}
	return queries.RepairRunProgress{
		ClusterID:         run.ClusterID.String(),
		TaskID:            run.TaskID.String(),
		RunID:             run.RunID.String(),
		Host:              run.Host,
		KeyspaceName:      run.Keyspace,
		TableName:         run.Table,
		CompletedAt:       completedAt,
		Duration:          int64(run.Duration),
		DurationStartedAt: durationStartedAt,
		Error:             run.Error,
		Size:              run.Size,
		StartedAt:         startedAt,
		Success:           run.Success,
		TokenRanges:       run.TokenRanges,
	}
}

func gocqlToSqlRunState(run RunState) queries.RepairRunState {
	return queries.RepairRunState{
		ClusterID:     run.ClusterID.String(),
		TaskID:        run.TaskID.String(),
		RunID:         run.RunID.String(),
		KeyspaceName:  run.Keyspace,
		TableName:     run.Table,
		SuccessRanges: []byte{1, 2, 3, 4, 5},
	}
}
