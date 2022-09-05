// Copyright (C) 2022 ScyllaDB

package backup

import (
	"context"
	"encoding/json"

	"github.com/pkg/errors"
	"github.com/scylladb/scylla-manager/v3/pkg/util/uuid"
)

// RestoreRunner implements scheduler.Runner.
type RestoreRunner struct {
	service *Service
}

// Run implementation for RestoreRunner.
func (r RestoreRunner) Run(ctx context.Context, clusterID, taskID, runID uuid.UUID, properties json.RawMessage) error {
	r.service.metrics.ResetClusterMetrics(clusterID)

	t, err := r.service.GetRestoreTarget(ctx, clusterID, properties)
	if err != nil {
		return errors.Wrap(err, "get restore target")
	}

	return r.service.Restore(ctx, clusterID, taskID, runID, t)
}

// RestoreRunner creates a RestoreRunner that handles restores.
func (s *Service) RestoreRunner() RestoreRunner {
	return RestoreRunner{service: s}
}
