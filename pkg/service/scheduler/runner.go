// Copyright (C) 2017 ScyllaDB

package scheduler

import (
	"context"
	"encoding/json"

	"github.com/scylladb/scylla-manager/pkg/util/uuid"
)

// Runner is a glue interface joining scheduler with agents doing the actual
// work. There can be one Runner per TaskType registered in scheduler Service.
// Run ID needs to be preserved whenever agent wants to persist the running
// state. The run can be cancelled by cancelling the context. If context is
// canceled runner must return error reported by the context.
type Runner interface {
	Run(ctx context.Context, clusterID, taskID, runID uuid.UUID, properties json.RawMessage) error
}
