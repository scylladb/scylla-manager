// Copyright (C) 2017 ScyllaDB

package scheduler

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/gocql/gocql"
	"github.com/pkg/errors"
	"github.com/scylladb/gocqlx/v2"
	"github.com/scylladb/scylla-manager/v3/pkg/scheduler"
	"github.com/scylladb/scylla-manager/v3/pkg/util"
	"github.com/scylladb/scylla-manager/v3/pkg/util/duration"
	"github.com/scylladb/scylla-manager/v3/pkg/util/retry"
	"github.com/scylladb/scylla-manager/v3/pkg/util/schedules"
	"github.com/scylladb/scylla-manager/v3/pkg/util/uuid"
	"go.uber.org/multierr"
)

// TaskType specifies the type of Task.
type TaskType string

// TaskType enumeration.
const (
	UnknownTask        TaskType = "unknown"
	BackupTask         TaskType = "backup"
	RestoreTask        TaskType = "restore"
	One2OneRestoreTask TaskType = "1_1_restore"
	HealthCheckTask    TaskType = "healthcheck"
	RepairTask         TaskType = "repair"
	SuspendTask        TaskType = "suspend"
	ValidateBackupTask TaskType = "validate_backup"

	mockTask TaskType = "mock"
)

func (t TaskType) String() string {
	return string(t)
}

func (t TaskType) MarshalText() (text []byte, err error) {
	return []byte(t.String()), nil
}

func (t *TaskType) UnmarshalText(text []byte) error {
	switch TaskType(text) {
	case UnknownTask:
		*t = UnknownTask
	case BackupTask:
		*t = BackupTask
	case RestoreTask:
		*t = RestoreTask
	case One2OneRestoreTask:
		*t = One2OneRestoreTask
	case HealthCheckTask:
		*t = HealthCheckTask
	case RepairTask:
		*t = RepairTask
	case SuspendTask:
		*t = SuspendTask
	case ValidateBackupTask:
		*t = ValidateBackupTask
	case mockTask:
		*t = mockTask
	default:
		return fmt.Errorf("unrecognized TaskType %q", text)
	}
	return nil
}

// AllowedTaskType is a wrapper around TaskType that allows it to be empty and adds additional validation.
type AllowedTaskType struct {
	TaskType
}

// IsEmpty returns true if the AllowedTaskType is empty.
func (t AllowedTaskType) IsEmpty() bool {
	return string(t.TaskType) == ""
}

func (t *AllowedTaskType) UnmarshalText(text []byte) error {
	if len(text) == 0 {
		return nil
	}
	var tt TaskType
	if err := tt.UnmarshalText(text); err != nil {
		return err
	}
	if tt == SuspendTask || tt == HealthCheckTask {
		return fmt.Errorf("allowed task type cannot be %q", tt)
	}
	*t = AllowedTaskType{tt}
	return nil
}

// WeekdayTime adds CQL capabilities to scheduler.WeekdayTime.
type WeekdayTime struct {
	scheduler.WeekdayTime
}

func (w WeekdayTime) MarshalCQL(info gocql.TypeInfo) ([]byte, error) {
	if i := info.Type(); i != gocql.TypeText && i != gocql.TypeVarchar {
		return nil, errors.Errorf("invalid gocql type %s expected %s", info.Type(), gocql.TypeText)
	}
	return w.MarshalText()
}

func (w *WeekdayTime) UnmarshalCQL(info gocql.TypeInfo, data []byte) error {
	if i := info.Type(); i != gocql.TypeText && i != gocql.TypeVarchar {
		return errors.Errorf("invalid gocql type %s expected %s", info.Type(), gocql.TypeText)
	}
	return w.UnmarshalText(data)
}

// Window adds JSON validation to scheduler.Window.
type Window []WeekdayTime

func (w *Window) UnmarshalJSON(data []byte) error {
	var wdt []scheduler.WeekdayTime
	if err := json.Unmarshal(data, &wdt); err != nil {
		return errors.Wrap(err, "window")
	}
	if len(wdt) == 0 {
		return nil
	}

	if _, err := scheduler.NewWindow(wdt...); err != nil {
		return errors.Wrap(err, "window")
	}

	s := make([]WeekdayTime, len(wdt))
	for i := range wdt {
		s[i] = WeekdayTime{wdt[i]}
	}
	*w = s

	return nil
}

// Window returns this window as scheduler.Window.
func (w Window) Window() scheduler.Window {
	if len(w) == 0 {
		return nil
	}
	wdt := make([]scheduler.WeekdayTime, len(w))
	for i := range w {
		wdt[i] = w[i].WeekdayTime
	}
	sw, _ := scheduler.NewWindow(wdt...) // nolint: errcheck
	return sw
}

// location adds CQL capabilities and validation to time.Location.
type location struct {
	*time.Location
}

func (l location) MarshalText() (text []byte, err error) {
	if l.Location == nil {
		return nil, nil
	}
	return []byte(l.Location.String()), nil
}

func (l *location) UnmarshalText(text []byte) error {
	if len(text) == 0 {
		return nil
	}
	t, err := time.LoadLocation(string(text))
	if err != nil {
		return err
	}
	l.Location = t
	return nil
}

func (l location) MarshalCQL(info gocql.TypeInfo) ([]byte, error) {
	if i := info.Type(); i != gocql.TypeText && i != gocql.TypeVarchar {
		return nil, errors.Errorf("invalid gocql type %s expected %s", info.Type(), gocql.TypeText)
	}
	return l.MarshalText()
}

func (l *location) UnmarshalCQL(info gocql.TypeInfo, data []byte) error {
	if i := info.Type(); i != gocql.TypeText && i != gocql.TypeVarchar {
		return errors.Errorf("invalid gocql type %s expected %s", info.Type(), gocql.TypeText)
	}
	return l.UnmarshalText(data)
}

// Timezone adds JSON validation to time.Location.
type Timezone struct {
	location
}

func NewTimezone(tz *time.Location) Timezone {
	return Timezone{location{tz}}
}

func (tz *Timezone) UnmarshalJSON(data []byte) error {
	return errors.Wrap(json.Unmarshal(data, &tz.location), "timezone")
}

// Location returns this timezone as time.Location pointer.
func (tz Timezone) Location() *time.Location {
	return tz.location.Location
}

// Schedule specify task schedule.
type Schedule struct {
	gocqlx.UDT `json:"-"`

	Cron      schedules.Cron `json:"cron"`
	Window    Window         `json:"window"`
	Timezone  Timezone       `json:"timezone"`
	StartDate time.Time      `json:"start_date"`
	// Deprecated: use cron instead
	Interval   duration.Duration `json:"interval" db:"interval_seconds"`
	NumRetries int               `json:"num_retries"`
	RetryWait  duration.Duration `json:"retry_wait"`
}

func (s Schedule) trigger() schedules.Trigger {
	if !s.Cron.IsZero() {
		return s.Cron
	}
	return schedules.NewLegacy(s.StartDate, s.Interval.Duration())
}

func (s Schedule) backoff() retry.Backoff {
	if s.NumRetries == 0 {
		return nil
	}
	w := s.RetryWait
	if w == 0 {
		w = duration.Duration(10 * time.Minute)
	}

	b := retry.NewExponentialBackoff(w.Duration(), 0, 3*time.Hour, 2, 0)
	b = retry.WithMaxRetries(b, uint64(s.NumRetries))
	return b
}

// Task specify task type, properties and schedule.
type Task struct {
	ClusterID  uuid.UUID         `json:"cluster_id"`
	Type       TaskType          `json:"type"`
	ID         uuid.UUID         `json:"id"`
	Name       string            `json:"name"`
	Labels     map[string]string `json:"labels"`
	Enabled    bool              `json:"enabled,omitempty"`
	Deleted    bool              `json:"deleted,omitempty"`
	Sched      Schedule          `json:"schedule,omitempty"`
	Properties json.RawMessage   `json:"properties,omitempty"`
	Tags       []string

	Status       Status     `json:"status"`
	SuccessCount int        `json:"success_count"`
	ErrorCount   int        `json:"error_count"`
	LastSuccess  *time.Time `json:"last_success"`
	LastError    *time.Time `json:"last_error"`
}

func (t *Task) String() string {
	return fmt.Sprintf("%s/%s", t.Type, t.ID)
}

func (t *Task) Validate() error {
	if t == nil {
		return util.ErrNilPtr
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

	return util.ErrValidate(errors.Wrap(errs, "invalid task"))
}

// Status specifies the status of a Task.
type Status string

// Status enumeration.
const (
	StatusNew      Status = "NEW"
	StatusRunning  Status = "RUNNING"
	StatusStopping Status = "STOPPING"
	StatusStopped  Status = "STOPPED"
	StatusWaiting  Status = "WAITING"
	StatusDone     Status = "DONE"
	StatusError    Status = "ERROR"
	StatusAborted  Status = "ABORTED"
)

var allStatuses = []Status{
	StatusNew,
	StatusRunning,
	StatusStopping,
	StatusStopped,
	StatusWaiting,
	StatusDone,
	StatusError,
	StatusAborted,
}

func (s Status) String() string {
	return string(s)
}

func (s Status) MarshalText() (text []byte, err error) {
	return []byte(s.String()), nil
}

func (s *Status) UnmarshalText(text []byte) error {
	switch Status(text) {
	case StatusNew:
		*s = StatusNew
	case StatusRunning:
		*s = StatusRunning
	case StatusStopping:
		*s = StatusStopping
	case StatusStopped:
		*s = StatusStopped
	case StatusWaiting:
		*s = StatusWaiting
	case StatusDone:
		*s = StatusDone
	case StatusError:
		*s = StatusError
	case StatusAborted:
		*s = StatusAborted
	default:
		return fmt.Errorf("unrecognized Status %q", text)
	}
	return nil
}

var healthCheckActiveRunID = uuid.NewFromTime(time.Unix(0, 0))

// Run describes a running instance of a Task.
type Run struct {
	ClusterID uuid.UUID  `json:"cluster_id"`
	Type      TaskType   `json:"type"`
	TaskID    uuid.UUID  `json:"task_id"`
	ID        uuid.UUID  `json:"id"`
	Status    Status     `json:"status"`
	Cause     string     `json:"cause,omitempty"`
	Owner     string     `json:"owner"`
	StartTime time.Time  `json:"start_time"`
	EndTime   *time.Time `json:"end_time,omitempty"`
}

// Duration checks if both EndTime and StartTime are set correctly and returns
// their difference formatted as a string. Otherwise, it returns "unknown" duration.
func (r Run) Duration() string {
	if r.EndTime != nil && !r.EndTime.IsZero() && !r.StartTime.IsZero() {
		return r.EndTime.Sub(r.StartTime).String()
	}
	return "unknown"
}

func newRunFromTaskInfo(ti taskInfo) *Run {
	var id uuid.UUID
	if ti.TaskType == HealthCheckTask {
		id = healthCheckActiveRunID
	} else {
		id = uuid.NewTime()
	}

	return &Run{
		ClusterID: ti.ClusterID,
		Type:      ti.TaskType,
		TaskID:    ti.TaskID,
		ID:        id,
		StartTime: now(),
		Status:    StatusRunning,
	}
}
