// Copyright (C) 2023 ScyllaDB

package restore

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
