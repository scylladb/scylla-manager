// Copyright (C) 2017 ScyllaDB

package runner

import (
	"context"
	"fmt"

	"github.com/scylladb/mermaid/uuid"
)

// Descriptor is a collection of IDs that let identify a task run.
type Descriptor struct {
	ClusterID uuid.UUID
	TaskID    uuid.UUID
	RunID     uuid.UUID
}

// Properties is a collection of string key-value pairs describing
// properties of a task.
type Properties map[string]string

// Status specifies the status of a Task.
type Status string

// Status enumeration.
const (
	StatusDone     Status = "done"
	StatusError    Status = "error"
	StatusStarting Status = "starting"
	StatusRunning  Status = "running"
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

//go:generate mockgen -destination ../schedrunner_mock.go -mock_names Runner=mockRunner -package sched github.com/scylladb/mermaid/sched/runner Runner

// Runner interface should be implemented by all services being triggered by
// scheduler.
type Runner interface {
	Run(ctx context.Context, d Descriptor, p Properties) error
	Stop(ctx context.Context, d Descriptor) error
	Status(ctx context.Context, d Descriptor) (Status, string, error)
}
