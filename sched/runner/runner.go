// Copyright (C) 2017 ScyllaDB

package runner

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/pkg/errors"
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
type Properties = json.RawMessage

// Status specifies the status of a Task.
type Status string

// Status enumeration.
const (
	StatusNew      Status = "NEW"
	StatusStarting Status = "STARTING"
	StatusRunning  Status = "RUNNING"
	StatusStopping Status = "STOPPING"
	StatusStopped  Status = "STOPPED"
	StatusDone     Status = "DONE"
	StatusError    Status = "ERROR"
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
	case StatusStarting:
		*s = StatusStarting
	case StatusRunning:
		*s = StatusRunning
	case StatusStopping:
		*s = StatusStopping
	case StatusStopped:
		*s = StatusStopped
	case StatusDone:
		*s = StatusDone
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

type nopRunner struct{}

func (nopRunner) Run(ctx context.Context, d Descriptor, p Properties) error {
	return errors.New("Nop runner")
}

func (nopRunner) Stop(ctx context.Context, d Descriptor) error {
	return errors.New("Nop runner")
}

func (nopRunner) Status(ctx context.Context, d Descriptor) (Status, string, error) {
	return "", "", errors.New("Nop runner")
}

// NopRunner is a runner implementation that does nothing and always returns
// error.
var NopRunner = nopRunner{}
