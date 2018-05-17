// Copyright (C) 2017 ScyllaDB

package repair

import (
	"context"
	"fmt"
	"strings"

	"github.com/pkg/errors"
	"github.com/scylladb/mermaid/sched/runner"
)

const (
	keyspaceKey = "keyspace"
	tablesKey   = "tables"
)

// Runner is an adapter to connect Service to scheduler.
type Runner struct {
	Service *Service
}

// Run implements runner.Runner.
func (r Runner) Run(ctx context.Context, d runner.Descriptor, p runner.Properties) error {
	var tables []string
	if p[tablesKey] != "" {
		tables = strings.Split(p[tablesKey], ",")
	}
	unit := Unit{
		Keyspace: p[keyspaceKey],
		Tables:   tables,
	}
	return r.Service.Repair(ctx, d.ClusterID, d.TaskID, d.RunID, unit)
}

// Stop implements runner.Runner.
func (r Runner) Stop(ctx context.Context, d runner.Descriptor) error {
	return r.Service.StopRepair(ctx, d.ClusterID, d.TaskID, d.RunID)
}

// Status implements runner.Runner.
func (r Runner) Status(ctx context.Context, d runner.Descriptor) (runner.Status, string, error) {
	run, err := r.Service.GetRun(ctx, d.ClusterID, d.TaskID, d.RunID)
	if err != nil {
		return "", "", errors.Wrap(err, "failed to load run")
	}
	switch run.Status {
	case StatusRunning, StatusStopping:
		return runner.StatusRunning, "", nil
	case StatusError:
		return runner.StatusError, run.Cause, nil
	case StatusDone, StatusStopped:
		return runner.StatusStopped, "", nil
	default:
		return "", "", fmt.Errorf("unsupported repair state %q", run.Status)
	}
}
