// Copyright (C) 2017 ScyllaDB

package repair

import (
	"context"

	"github.com/pkg/errors"
	"github.com/scylladb/mermaid/sched/runner"
)

type repairRunner struct {
	service *Service
}

// Run implements runner.Runner.
func (r repairRunner) Run(ctx context.Context, d runner.Descriptor, p runner.Properties) error {
	t, err := r.service.GetTarget(ctx, d.ClusterID, p)
	if err != nil {
		return errors.Wrap(err, "failed to load units")
	}

	return r.service.Repair(ctx, d.ClusterID, d.TaskID, d.RunID, t)
}

// Stop implements runner.Runner.
func (r repairRunner) Stop(ctx context.Context, d runner.Descriptor) error {
	return r.service.StopRepair(ctx, d.ClusterID, d.TaskID, d.RunID)
}

// Status implements runner.Runner.
func (r repairRunner) Status(ctx context.Context, d runner.Descriptor) (runner.Status, string, error) {
	run, err := r.service.GetRun(ctx, d.ClusterID, d.TaskID, d.RunID)
	if err != nil {
		return "", "", errors.Wrap(err, "failed to load run")
	}
	return run.Status, run.Cause, nil
}
