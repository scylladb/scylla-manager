// Copyright (C) 2025 ScyllaDB

package one2onerestore

import (
	"context"
	"encoding/json"

	"github.com/scylladb/scylla-manager/v3/pkg/util/uuid"
)

// Runner implements scheduler.Runner.
type Runner struct {
	service *Service
}

// Run implementation for Runner.
func (r Runner) Run(ctx context.Context, clusterID, taskID, runID uuid.UUID, properties json.RawMessage) error {
	return r.service.One2OneRestore(ctx, clusterID, taskID, runID, properties)
}

// Runner creates a Runner that handles restores.
func (s *Service) Runner() Runner {
	return Runner{service: s}
}
