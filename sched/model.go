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
	UnknownTask            TaskType = "unknown"
	BackupTask             TaskType = "backup"
	RepairAutoScheduleTask TaskType = "repair_auto_schedule"
	RepairTask             TaskType = "repair"

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
	case RepairAutoScheduleTask:
		*t = RepairAutoScheduleTask
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
	StartDate    time.Time `json:"start_date"`
	IntervalDays int       `json:"interval_days"`
	NumRetries   int       `json:"num_retries"`
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

func (s *Schedule) nextActivation(now time.Time, runs []*Run) time.Time {
	n := len(runs)
	if n == 0 && s.StartDate.After(now.Add(taskStartNowSlack)) {
		return s.StartDate
	}
	lastStart := s.StartDate
	lastStatus := runner.StatusError
	if n > 0 {
		lastStart = runs[0].StartTime
		lastStatus = runs[0].Status
	}

	if s.NumRetries > 0 {
		// check no more than NumRetries Runs were attempted
		retries := 0
		for _, r := range runs {
			if r.Status == runner.StatusStopped {
				break
			}
			retries++
		}
		if lastStatus != runner.StatusStopped && retries < s.NumRetries {
			t := lastStart.Add(retryTaskWait)
			if t.Before(now) {
				// previous activation was is in the past, and didn't occur. Try again now.
				return now.Add(taskStartNowSlack)
			}
			return t
		}
	}

	// advance start date with idealized periods
	if s.IntervalDays > 0 {
		for lastStart.Before(now) {
			lastStart = lastStart.AddDate(0, 0, s.IntervalDays)
		}
		return lastStart
	}
	return time.Time{}
}

// Task is a schedulable entity.
type Task struct {
	ClusterID uuid.UUID `json:"cluster_id"`
	Type      TaskType  `json:"type"`
	ID        uuid.UUID `json:"id"`
	Name      string    `json:"name"`
	Tags      []string  `json:"tags"`
	Metadata  string    `json:"metadata"`
	Enabled   bool      `json:"enabled"`
	Sched     Schedule  `json:"schedule"`

	Properties map[string]string `json:"properties"`
}

// Validate checks if all the required fields are properly set.
func (t *Task) Validate() (err error) {
	if t == nil {
		return mermaid.ErrNilPtr
	}

	if t.ID == uuid.Nil {
		err = multierr.Append(err, errors.New("missing ID"))
	}
	if t.ClusterID == uuid.Nil {
		err = multierr.Append(err, errors.New("missing ClusterID"))
	}
	if _, e := uuid.Parse(t.Name); e == nil {
		err = multierr.Append(err, errors.New("name cannot be an UUID"))
	}

	switch t.Type {
	case "", UnknownTask:
		err = multierr.Append(err, errors.New("no TaskType specified"))
	default:
		var tp TaskType
		err = multierr.Append(err, tp.UnmarshalText([]byte(t.Type)))
	}

	if t.Sched.IntervalDays < 0 {
		err = multierr.Append(err, errors.New("negative Sched.IntervalDays"))
	}
	if t.Sched.NumRetries < 0 {
		err = multierr.Append(err, errors.New("negative Sched.NumRetries"))
	}
	if t.Sched.StartDate.IsZero() {
		err = multierr.Append(err, errors.New("missing Sched.StartDate"))
	}

	return
}

// Run describes a running instance of a Task.
type Run struct {
	ID        uuid.UUID     `json:"id"`
	Type      TaskType      `json:"type"`
	ClusterID uuid.UUID     `json:"cluster_id"`
	TaskID    uuid.UUID     `json:"task_id"`
	Status    runner.Status `json:"status"`
	Cause     string        `json:"cause"`
	Owner     string        `json:"owner"`
	StartTime time.Time     `json:"start_time"`
	EndTime   time.Time     `json:"end_time"`
}
