// Copyright (C) 2017 ScyllaDB

package sched

import (
	"errors"
	"fmt"
	"reflect"
	"time"

	"github.com/gocql/gocql"
	"github.com/scylladb/gocqlx"
	"github.com/scylladb/mermaid"
	"github.com/scylladb/mermaid/sched/runner"
	"github.com/scylladb/mermaid/uuid"
	"go.uber.org/multierr"
)

// TaskType specifies the type of a Task.
type TaskType string

// TaskType enumeration
const (
	UnknownTask     TaskType = "unknown"
	BackupTask      TaskType = "backup"
	HealthCheckTask TaskType = "healthcheck"
	RepairTask      TaskType = "repair"

	mockTask TaskType = "mock"
)

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
	case HealthCheckTask:
		*t = HealthCheckTask
	case RepairTask:
		*t = RepairTask
	case mockTask:
		*t = mockTask
	default:
		return fmt.Errorf("unrecognized TaskType %q", text)
	}
	return nil
}

// Schedule defines a periodic schedule.
type Schedule struct {
	StartDate  time.Time `json:"start_date"`
	Interval   Duration  `json:"interval" db:"interval_seconds"`
	NumRetries int       `json:"num_retries"`
}

// MarshalUDT implements UDTMarshaler.
func (s Schedule) MarshalUDT(name string, info gocql.TypeInfo) ([]byte, error) {
	f := gocqlx.DefaultMapper.FieldByName(reflect.ValueOf(s), name)
	return gocql.Marshal(info, f.Interface())
}

// UnmarshalUDT implements UDTUnmarshaler.
func (s *Schedule) UnmarshalUDT(name string, info gocql.TypeInfo, data []byte) error {
	f := gocqlx.DefaultMapper.FieldByName(reflect.ValueOf(s), name)
	return gocql.Unmarshal(info, data, f.Addr().Interface())
}

// NextActivation generates new start time based on schedule and run history.
func (s *Schedule) NextActivation(now time.Time, runs []*Run) time.Time {
	// if not started yet report scheduled start date
	if len(runs) == 0 && s.StartDate.After(now.Add(taskStartNowSlack)) {
		return s.StartDate
	}

	lastStart := s.StartDate
	lastStatus := runner.StatusError
	if len(runs) > 0 {
		lastStart = runs[0].StartTime
		lastStatus = runs[0].Status
	}

	switch lastStatus {
	case runner.StatusAborted:
		// skip, always retry aborted
	case runner.StatusError:
		// if no retries available report next activation according to schedule
		if s.consecutiveErrorCount(runs) > s.NumRetries {
			return s.nextActivation(now)
		}
	default:
		// if running or done report next activation according to schedule
		return s.nextActivation(now)
	}

	// if retries available add retryTaskWait
	t := lastStart.Add(retryTaskWait)
	if t.Before(now) {
		// previous activation was is in the past, and didn't occur, try again now
		return now.Add(taskStartNowSlack)
	}
	return t
}

func (s *Schedule) consecutiveErrorCount(runs []*Run) int {
	errs := 0
	for _, r := range runs {
		if r.Status != runner.StatusError {
			break
		}
		errs++
	}
	return errs
}

func (s *Schedule) nextActivation(now time.Time) time.Time {
	if s.Interval > 0 {
		lastStart := s.StartDate.Add(now.Sub(s.StartDate).Round(s.Interval.Duration()))
		for lastStart.Before(now) {
			lastStart = lastStart.Add(s.Interval.Duration())
		}
		return lastStart
	}
	return time.Time{}
}

// Task is a schedulable entity.
type Task struct {
	ClusterID  uuid.UUID         `json:"cluster_id"`
	Type       TaskType          `json:"type"`
	ID         uuid.UUID         `json:"id"`
	Name       string            `json:"name"`
	Tags       []string          `json:"tags"`
	Enabled    bool              `json:"enabled"`
	Sched      Schedule          `json:"schedule"`
	Properties runner.Properties `json:"properties"`

	clusterName string
}

// Validate checks if all the required fields are properly set.
func (t *Task) Validate() error {
	if t == nil {
		return mermaid.ErrNilPtr
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
	if t.Sched.StartDate.IsZero() {
		errs = multierr.Append(errs, errors.New("missing start date"))
	}

	return mermaid.ErrValidate(errs, "invalid task")
}

// Run describes a running instance of a Task.
type Run struct {
	ID        uuid.UUID     `json:"id"`
	Type      TaskType      `json:"type"`
	ClusterID uuid.UUID     `json:"cluster_id"`
	TaskID    uuid.UUID     `json:"task_id"`
	Status    runner.Status `json:"status"`
	Cause     string        `json:"cause,omitempty"`
	Owner     string        `json:"owner"`
	StartTime time.Time     `json:"start_time"`
	EndTime   *time.Time    `json:"end_time,omitempty"`
}

// Descriptor returns descriptor of this Run.
func (r *Run) Descriptor() runner.Descriptor {
	return runner.Descriptor{
		ClusterID: r.ClusterID,
		TaskID:    r.TaskID,
		RunID:     r.ID,
	}
}
