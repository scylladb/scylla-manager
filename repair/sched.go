// Copyright (C) 2017 ScyllaDB

package repair

import (
	"context"
	"fmt"

	"github.com/pkg/errors"
	"github.com/scylladb/mermaid/sched/runner"
	"github.com/scylladb/mermaid/uuid"
)

// Run implements sched/runner.Runner.
func (s *Service) Run(ctx context.Context, clusterID, runID uuid.UUID, props runner.TaskProperties) error {
	u, err := s.GetUnit(ctx, clusterID, props["unit_id"])
	if err != nil {
		return errors.Wrapf(err, "failed to load unit %q", props["unit_id"])
	}
	return s.Repair(ctx, u, runID)
}

// Stop implements sched/runner.Runner.
func (s *Service) Stop(ctx context.Context, clusterID, runID uuid.UUID, props runner.TaskProperties) error {
	u, err := s.GetUnit(ctx, clusterID, props["unit_id"])
	if err != nil {
		return errors.Wrapf(err, "failed to load unit %q", props["unit_id"])
	}
	return s.StopRun(ctx, u, runID)
}

// Status implements sched/runner.Runner.
func (s *Service) Status(ctx context.Context, clusterID, runID uuid.UUID, props runner.TaskProperties) (runner.Status, string, error) {
	u, err := s.GetUnit(ctx, clusterID, props["unit_id"])
	if err != nil {
		return "", "", errors.Wrapf(err, "failed to load unit %q", props["unit_id"])
	}
	run, err := s.GetRun(ctx, u, runID)
	if err != nil {
		return "", "", errors.Wrapf(err, "failed to load run %q", runID)
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
