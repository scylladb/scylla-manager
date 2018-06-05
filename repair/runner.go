// Copyright (C) 2017 ScyllaDB

package repair

import (
	"context"
	"encoding/json"
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
	var (
		tables []string
		m      map[string]string
	)

	if err := json.Unmarshal(p, &m); err != nil {
		return errors.Wrapf(err, "unable to parse runner properties: %v", p)
	}

	if m[tablesKey] != "" {
		tables = strings.Split(m[tablesKey], ",")
	}
	u := Unit{
		Keyspace: m[keyspaceKey],
		Tables:   tables,
	}

	return r.Service.Repair(ctx, d.ClusterID, d.TaskID, d.RunID, []Unit{u})
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
	case runner.StatusRunning, runner.StatusStopping:
		return runner.StatusRunning, "", nil
	case runner.StatusError:
		return runner.StatusError, run.Cause, nil
	case runner.StatusDone, runner.StatusStopped:
		return runner.StatusStopped, "", nil
	default:
		return "", "", fmt.Errorf("unsupported repair state %q", run.Status)
	}
}
