// Copyright (C) 2017 ScyllaDB

package backup

import (
	"context"
	"encoding/json"

	"github.com/pkg/errors"
	"github.com/scylladb/mermaid/uuid"
)

// Runner implements sched.Runner.
type Runner struct {
	service *Service
}

func (r Runner) Run(ctx context.Context, clusterID, taskID, runID uuid.UUID, properties json.RawMessage) error {
	t, err := r.service.GetTarget(ctx, clusterID, properties, false)
	if err != nil {
		return errors.Wrap(err, "failed to get backup target")
	}

	return r.service.Backup(ctx, clusterID, taskID, runID, t)
}
