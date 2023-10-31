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
	r.service.metrics.ResetClusterMetrics(clusterID)

	t, err := r.service.GetTarget(ctx, clusterID, properties)
	if err != nil {
		return errors.Wrap(err, "get backup target")
	}

	return r.service.Backup(ctx, clusterID, taskID, runID, t)
}
