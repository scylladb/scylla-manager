// Copyright (C) 2017 ScyllaDB

package backup

import (
	"context"
	"encoding/json"

	"github.com/pkg/errors"
	"github.com/scylladb/scylla-manager/v3/pkg/util/uuid"
)

// Runner implements scheduler.Runner.
type Runner struct {
	service *Service
}

func (r Runner) Run(ctx context.Context, clusterID, taskID, runID uuid.UUID, properties json.RawMessage) error {
	// Both resets belong here, at the very start of the run. Resetting the
	// task later - once the snapshot is taken, say - leaves the progress of
	// the previous run on display while the new one is already reported as
	// running, so a task that has just started reads as complete.
	r.service.metrics.ResetClusterMetrics(clusterID)
	r.service.metrics.ResetTaskMetrics(clusterID, taskID)
	r.service.setTaskPropertiesMetric(ctx, clusterID, taskID, properties)

	t, err := r.service.GetTarget(ctx, clusterID, properties)
	if err != nil {
		return errors.Wrap(err, "get backup target")
	}

	return r.service.Backup(ctx, clusterID, taskID, runID, t)
}
