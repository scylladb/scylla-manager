// Copyright (C) 2025 ScyllaDB

package tablet

import (
	"context"
	"encoding/json"

	"github.com/pkg/errors"
	"github.com/scylladb/go-log"
	"github.com/scylladb/gocqlx/v2"
	"github.com/scylladb/scylla-manager/v3/pkg/metrics"
	"github.com/scylladb/scylla-manager/v3/pkg/scyllaclient"
	"github.com/scylladb/scylla-manager/v3/pkg/util/uuid"
)

// Service for the tablet repair.
type Service struct {
	logger       log.Logger
	metrics      metrics.TabletRepairMetrics
	smSession    gocqlx.Session
	scyllaClient scyllaclient.ProviderFunc
}

// NewService creates new tablet repair service.
func NewService(smSession gocqlx.Session, metrics metrics.TabletRepairMetrics, scyllaClient scyllaclient.ProviderFunc, logger log.Logger) *Service {
	return &Service{
		logger:       logger.Named("tablet_repair"),
		metrics:      metrics,
		smSession:    smSession,
		scyllaClient: scyllaClient,
	}
}

// GetTarget of tablet repair task.
func (s *Service) GetTarget(ctx context.Context, clusterID uuid.UUID, _ json.RawMessage) (Target, error) {
	w, err := s.newTargetWorker(ctx, clusterID)
	if err != nil {
		return Target{}, errors.Wrap(err, "new target worker")
	}
	target, err := w.getTarget(ctx)
	if err != nil {
		return Target{}, errors.Wrap(err, "get target")
	}
	return target, nil
}

// Run tablet repair task.
func (s *Service) Run(ctx context.Context, clusterID, taskID, runID uuid.UUID, properties json.RawMessage) error {
	target, err := s.GetTarget(ctx, clusterID, properties)
	if err != nil {
		return errors.Wrap(err, "get target")
	}

	w, err := s.newWorker(ctx, clusterID, taskID, runID)
	if err != nil {
		return errors.Wrap(err, "new worker")
	}
	if err := w.repairAll(ctx, target); err != nil {
		return errors.Wrap(err, "repair all tablet tables")
	}
	return nil
}

// GetProgress of tablet repair task.
func (s *Service) GetProgress(_ context.Context, clusterID, taskID, runID uuid.UUID) (Progress, error) {
	w := s.newProgressWorker(clusterID, taskID, runID)
	pr, err := w.getProgress()
	if err != nil {
		return Progress{}, errors.Wrap(err, "get progress")
	}
	return pr, nil
}
