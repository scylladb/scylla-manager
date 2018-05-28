// Copyright (C) 2017 ScyllaDB

package runner

import (
	"context"
	"fmt"

	"github.com/scylladb/mermaid/uuid"
)

// TaskProperties is a collection of string key-value pairs describing properties of a task.
type TaskProperties map[string]string

// Status specifies the status of a Task.
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

//go:generate mockgen -source=runner.go  -destination ../schedrunner_mock.go -mock_names Runner=mockRunner -package sched

// Runner interface should be implemented by all services being triggered by the scheduler service.
type Runner interface {
	Run(ctx context.Context, clusterID, runID uuid.UUID, props TaskProperties) error
	Stop(ctx context.Context, clusterID, runID uuid.UUID, props TaskProperties) error
	Status(ctx context.Context, clusterID, runID uuid.UUID, props TaskProperties) (Status, string, error)
}
