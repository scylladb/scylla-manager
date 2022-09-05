// Copyright (C) 2022 ScyllaDB

package backup

import (
	"context"
	"encoding/json"

	"github.com/scylladb/scylla-manager/v3/pkg/util/uuid"
)

func (s *Service) Restore(ctx context.Context, clusterID, taskID, runID uuid.UUID, target RestoreTarget) error {
	panic("TODO - implement")
}

func (s *Service) GetRestoreTarget(ctx context.Context, clusterID uuid.UUID, properties json.RawMessage) (RestoreTarget, error) {
	panic("TODO - implement")
}
