// Copyright (C) 2017 ScyllaDB

package scheduler

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/pkg/errors"
	"github.com/scylladb/gocqlx/v2"
	"github.com/scylladb/scylla-manager/pkg/service"
	"github.com/scylladb/scylla-manager/pkg/store"
	"github.com/scylladb/scylla-manager/pkg/util/duration"
	"github.com/scylladb/scylla-manager/pkg/util/timeutc"
	"github.com/scylladb/scylla-manager/pkg/util/uuid"
	"go.uber.org/multierr"
)

// TaskType specifies the type of a Task.
type TaskType string

// TaskType enumeration
const (
	UnknownTask               TaskType = "unknown"
	BackupTask                TaskType = "backup"
	HealthCheckAlternatorTask TaskType = "healthcheck_alternator"
	HealthCheckCQLTask        TaskType = "healthcheck"
	HealthCheckRESTTask       TaskType = "healthcheck_rest"
	RepairTask                TaskType = "repair"

	mockTask TaskType = "mock"
)

// IgnoreSuspended returns true if task should run even when scheduler is suspended.
func (t TaskType) IgnoreSuspended() bool {
	switch t {
	case HealthCheckAlternatorTask, HealthCheckCQLTask, HealthCheckRESTTask:
		return true
	}
	return false
}

func (t TaskType) String() string {
	return string(t)
}

// MarshalText implements encoding.TextMarshaler.
func (t TaskType) MarshalText() (text []byte, err error) {
	return []byte(t.String()), nil
}

// UnmarshalText implements encoding.TextUnmarshaler.
func (t *TaskType) UnmarshalText(text []byte) error {
	switch TaskType(text) {
	case UnknownTask:
		*t = UnknownTask
	case BackupTask:
		*t = BackupTask
	case HealthCheckAlternatorTask:
		*t = HealthCheckAlternatorTask
	case HealthCheckCQLTask:
		*t = HealthCheckCQLTask
	case HealthCheckRESTTask:
		*t = HealthCheckRESTTask
	case RepairTask:
		*t = RepairTask
	case mockTask:
		*t = mockTask
	default:
		return fmt.Errorf("unrecognized TaskType %q", text)
	}
	return nil
}

// Status specifies the status of a Task.
type Status string

// Status enumeration.
const (
	StatusNew     Status = "NEW"
	StatusRunning Status = "RUNNING"
	StatusStopped Status = "STOPPED"
	StatusDone    Status = "DONE"
	StatusError   Status = "ERROR"

	StatusAborted Status = "ABORTED"
	StatusMissed  Status = "MISSED"
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
	case StatusNew:
		*s = StatusNew
	case StatusRunning:
		*s = StatusRunning
	case StatusStopped:
		*s = StatusStopped
	case StatusDone:
		*s = StatusDone
	case StatusError:
		*s = StatusError
	case StatusAborted:
		*s = StatusAborted
	case StatusMissed:
		*s = StatusMissed
	default:
		return fmt.Errorf("unrecognized Status %q", text)
	}
	return nil
}

// Run describes a running instance of a Task.
type Run struct {
	ID        uuid.UUID  `json:"id"`
	Type      TaskType   `json:"type"`
	ClusterID uuid.UUID  `json:"cluster_id"`
	TaskID    uuid.UUID  `json:"task_id"`
	Status    Status     `json:"status"`
	Cause     string     `json:"cause,omitempty"`
	Owner     string     `json:"owner"`
	StartTime time.Time  `json:"start_time"`
	EndTime   *time.Time `json:"end_time,omitempty"`
}

// Schedule defines a periodic schedule.
type Schedule struct {
	gocqlx.UDT

	StartDate  time.Time         `json:"start_date"`
	Interval   duration.Duration `json:"interval" db:"interval_seconds"`
	NumRetries int               `json:"num_retries"`
}

// NextActivation generates new start time based on schedule and run history.
func (s *Schedule) NextActivation(now time.Time, suspended bool, runs []*Run) time.Time {
	if len(runs) == 0 {
		var a time.Time
		switch {
		// Start time in future just use that.
		case s.StartDate.After(now):
			a = s.StartDate
		// Cluster is suspended one-offs are not activated and normal runs
		// follow the next activation.
		case suspended:
			a = s.nextActivation(now)
		// Start date is just slightly off run it now.
		default:
			a = now
		}
		return a
	}

	var (
		lastStart  = s.StartDate
		lastStatus = StatusError
	)
	if len(runs) > 0 {
		lastStart = runs[0].StartTime
		lastStatus = runs[0].Status
	}
	switch lastStatus {
	case StatusAborted:
		// Skip, always retry aborted
	case StatusError:
		// If no retries available report next activation according to schedule.
		if s.ConsecutiveErrorCount(runs, now) > s.NumRetries {
			return s.nextActivation(now)
		}
	default:
		// If running or done report next activation according to schedule
		return s.nextActivation(now)
	}

	// If retries available add retryTaskWait
	t := lastStart.Add(retryTaskWait)
	if t.Before(now) {
		// previous activation was is in the past, and didn't occur, try again now
		return now
	}
	return t
}

// ConsecutiveErrorCount returns the number of consecutive errors happened before now.
// If Schedule.Interval is zero then all provided runs will be considered for counting.
// Othervise only those runs started within half of the Schedule.Inteval before now will
// be considered for counting.
func (s *Schedule) ConsecutiveErrorCount(runs []*Run, now time.Time) int {
	threshold := time.Time{}
	if s.Interval != 0 {
		// limit consecutive errors to current interval
		threshold = now.Add(-s.Interval.Duration() / 2)
	}
	errs := 0
	for _, r := range runs {
		if r.Status != StatusError {
			break
		}
		if r.StartTime.Before(threshold) {
			break
		}
		errs++
	}
	return errs
}

func (s *Schedule) nextActivation(now time.Time) time.Time {
	if s.Interval == 0 {
		return time.Time{}
	}

	lastStart := s.StartDate.Add(now.Sub(s.StartDate).Round(s.Interval.Duration()))
	for lastStart.Before(now) {
		lastStart = lastStart.Add(s.Interval.Duration())
	}
	return lastStart
}

// Task is a schedulable entity.
type Task struct {
	ClusterID  uuid.UUID  `json:"cluster_id"`
	Type       TaskType   `json:"type"`
	ID         uuid.UUID  `json:"id"`
	Name       string     `json:"name"`
	Tags       []string   `json:"tags"`
	Enabled    bool       `json:"enabled"`
	Sched      Schedule   `json:"schedule"`
	Properties Properties `json:"properties"`

	opts []Opt
}

// Validate checks if all the required fields are properly set.
func (t *Task) Validate() error {
	if t == nil {
		return service.ErrNilPtr
	}

	var errs error
	if t.ID == uuid.Nil {
		errs = multierr.Append(errs, errors.New("missing ID"))
	}
	if t.ClusterID == uuid.Nil {
		errs = multierr.Append(errs, errors.New("missing ClusterID"))
	}
	if _, e := uuid.Parse(t.Name); e == nil {
		errs = multierr.Append(errs, errors.New("name cannot be an UUID"))
	}

	switch t.Type {
	case "", UnknownTask:
		errs = multierr.Append(errs, errors.New("no TaskType specified"))
	default:
		var tp TaskType
		errs = multierr.Append(errs, tp.UnmarshalText([]byte(t.Type)))
	}

	if t.Sched.Interval < 0 {
		errs = multierr.Append(errs, errors.New("negative interval days"))
	}
	if t.Sched.NumRetries < 0 {
		errs = multierr.Append(errs, errors.New("negative num retries"))
	}
	// The Interval has to be greater than at least twice the time that retries
	// can be applied for i.e. Interval > NumRetries*retryTaskWait*2
	if t.Sched.Interval.Duration() > 0 && !(t.Sched.Interval.Duration() > time.Duration(t.Sched.NumRetries)*retryTaskWait*2) {
		errs = multierr.Append(errs, errors.Errorf("a task with %d retries needs to have interval greater than %s",
			t.Sched.NumRetries, time.Duration(t.Sched.NumRetries)*retryTaskWait*2))
	}
	if t.Sched.StartDate.IsZero() {
		errs = multierr.Append(errs, errors.New("missing start date"))
	}

	return service.ErrValidate(errors.Wrap(errs, "invalid task"))
}

func (t *Task) newRun(id uuid.UUID) *Run {
	return &Run{
		ID:        id,
		Type:      t.Type,
		ClusterID: t.ClusterID,
		TaskID:    t.ID,
		Status:    StatusNew,
		StartTime: timeutc.Now(),
	}
}

type suspendInfo struct {
	ClusterID    uuid.UUID   `json:"-"`
	StartedAt    time.Time   `json:"started_at"`
	PendingTasks []uuid.UUID `json:"pending_tasks"`
	RunningTask  []uuid.UUID `json:"running_tasks"`
}

var _ store.Entry = &suspendInfo{}

func (v *suspendInfo) Key() (clusterID uuid.UUID, key string) {
	return v.ClusterID, "scheduler_suspended"
}

func (v *suspendInfo) MarshalBinary() (data []byte, err error) {
	return json.Marshal(v)
}

func (v *suspendInfo) UnmarshalBinary(data []byte) error {
	return json.Unmarshal(data, v)
}
