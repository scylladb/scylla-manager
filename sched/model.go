// Copyright (C) 2017 ScyllaDB

package sched

import (
	"errors"
	"fmt"
	"reflect"
	"time"

	"github.com/gocql/gocql"
	"github.com/scylladb/gocqlx"
	"github.com/scylladb/mermaid/uuid"
)

// TaskType specifies the type of a Task.
type TaskType string

// TaskType enumeration
const (
	UnknownTask TaskType = "unknown"
	BackupTask  TaskType = "backup"
	RepairTask  TaskType = "repair"
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
	case RepairTask:
		*t = RepairTask
	default:
		return fmt.Errorf("unrecognized TaskType %q", text)
	}
	return nil
}

// Schedule defines a periodic schedule.
type Schedule struct {
	Repeat       bool      `json:"repeat"`
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
	lastStart := s.StartDate
	if n := len(runs); n > 0 {
		lastStart = runs[n-1].StartTime
	}

	if s.NumRetries > 0 {
		// check no more than NumRetries Runs were attempted
		retries := 0
		for i := len(runs) - 1; i >= 0; i-- {
			retries++
			if runs[i].Status == StatusStopped {
				break
			}
		}
		if retries < s.NumRetries {
			t := lastStart.Add(time.Hour)
			if t.Before(now) {
				return now.Add(time.Hour)
			}
			return t
		}
	}

	// advance start date with idealized periods
	for lastStart.Before(now) {
		lastStart = lastStart.AddDate(0, 0, s.IntervalDays)
	}
	return lastStart
}

// Task is a schedulable entity.
type Task struct {
	ClusterID  uuid.UUID `json:"cluster_id"`
	Type       TaskType  `json:"type"`
	ExternalID string    `json:"external_id"`
	Tags       []string  `json:"tags"`
	Metadata   string    `json:"metadata"`
	Enabled    bool      `json:"enabled"`
	Sched      Schedule  `json:"schedule"`

	Properties map[string]string `json:"properties"`
}

// Validate checks if all the required fields are properly set.
func (t *Task) Validate() error {
	if t == nil {
		return errors.New("nil task")
	}
	if t.ExternalID == "" {
		return errors.New("empty external ID")
	}
	if t.ClusterID == uuid.Nil {
		return errors.New("missing ClusterID")
	}
	switch t.Type {
	case BackupTask, RepairTask:

	case UnknownTask:
		return errors.New("no TaskType specified")

	default:
		return fmt.Errorf("unrecognized TaskType %q", t.Type)
	}
	return nil
}

// Status specifies the status of a Run.
type Status string

// Status enumeration.
const (
	StatusStarting Status = "starting"
	StatusRunning  Status = "running"
	StatusStopping Status = "stopping"
	StatusStopped  Status = "stopped"
	StatusError    Status = "error"
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
	case StatusStarting:
		*s = StatusStarting
	case StatusRunning:
		*s = StatusRunning
	case StatusStopping:
		*s = StatusStopping
	case StatusStopped:
		*s = StatusStopped
	case StatusError:
		*s = StatusError
	default:
		return fmt.Errorf("unrecognized Status %q", text)
	}
	return nil
}

// Run describes a running instance of a Task.
type Run struct {
	ID         uuid.UUID `json:"id"`
	Type       TaskType  `json:"type"`
	ClusterID  uuid.UUID `json:"cluster_id"`
	ExternalID string    `json:"external_id"`
	Status     Status    `json:"status"`
	Cause      string    `json:"cause"`
	Owner      string    `json:"owner"`
	StartTime  time.Time `json:"start_time"`
	EndTime    time.Time `json:"end_time"`
}
