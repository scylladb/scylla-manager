// Copyright (C) 2025 ScyllaDB

package one2onerestore

import (
	"context"
	"encoding/json"

	"github.com/pkg/errors"
	"github.com/scylladb/go-log"
	"github.com/scylladb/gocqlx/v2"

	"github.com/scylladb/scylla-manager/v3/pkg/scyllaclient"
	"github.com/scylladb/scylla-manager/v3/pkg/service/cluster"
	"github.com/scylladb/scylla-manager/v3/pkg/service/configcache"
	"github.com/scylladb/scylla-manager/v3/pkg/util/timeutc"
	"github.com/scylladb/scylla-manager/v3/pkg/util/uuid"
)

// Servicer is an interface that defines one2onerestore service API contract.
type Servicer interface {
	// One2OneRestore restores data (tables) from the source cluster backup to the target cluster if they have equal topology:
	// nodes count, shards count, token ownership should be exactly the same.
	One2OneRestore(ctx context.Context, clusterID, taskID, runID uuid.UUID, properties json.RawMessage) error
	// Runner creates a Runner that handles 1-1-restore operations.
	Runner() Runner

	// GetProgress returns progress of the 1-1-restore task run identified by runID.
	GetProgress(ctx context.Context, clusterID, taskID, runID uuid.UUID, properties json.RawMessage) (Progress, error)
}

// Service for the 1-1-restore.
type Service struct {
	session gocqlx.Session

	scyllaClient   scyllaclient.ProviderFunc
	clusterSession cluster.SessionFunc
	configCache    configcache.ConfigCacher
	logger         log.Logger
}

func NewService(session gocqlx.Session, scyllaClient scyllaclient.ProviderFunc, clusterSession cluster.SessionFunc, configCache configcache.ConfigCacher,
	logger log.Logger,
) (Servicer, error) {
	if session.Session == nil || session.Closed() {
		return nil, errors.New("invalid session")
	}

	if scyllaClient == nil {
		return nil, errors.New("invalid scylla provider")
	}

	return &Service{
		session: session,

		scyllaClient:   scyllaClient,
		clusterSession: clusterSession,
		configCache:    configCache,

		logger: logger,
	}, nil
}

// One2OneRestore creates and initializes worker performing 1-1-restore with given properties.
func (s *Service) One2OneRestore(ctx context.Context, clusterID, taskID, runID uuid.UUID, properties json.RawMessage) error {
	s.logger.Info(ctx, "1-1-restore",
		"cluster_id", clusterID,
		"task_id", taskID,
		"run_id", runID,
	)

	w, err := s.newWorker(ctx, clusterID, taskID, runID)
	if err != nil {
		return errors.Wrap(err, "new worker")
	}

	target, err := w.parseTarget(ctx, properties)
	if err != nil {
		return errors.Wrap(err, "parse target")
	}
	s.logger.Info(ctx, "Service input params", "target", target)

	manifests, hosts, err := w.getAllSnapshotManifestsAndTargetHosts(ctx, target)
	if err != nil {
		return errors.Wrap(err, "get manifests and hosts info")
	}

	if err := w.validateClusters(ctx, manifests, hosts, target.NodesMapping); err != nil {
		return errors.Wrap(err, "validate clusters")
	}
	s.logger.Info(ctx, "Can proceed with 1-1-restore")

	workload, err := w.prepareHostWorkload(ctx, manifests, hosts, target)
	if err != nil {
		return errors.Wrap(err, "prepare hosts workload")
	}

	if err := w.initProgress(ctx, workload); err != nil {
		return errors.Wrap(err, "init progress")
	}

	start := timeutc.Now()
	if err := w.restore(ctx, workload, target); err != nil {
		return errors.Wrap(err, "restore data")
	}
	s.logger.Info(ctx, "Data restore is completed", "took", timeutc.Since(start))
	return nil
}

func (s *Service) newWorker(ctx context.Context, clusterID, taskID, runID uuid.UUID) (worker, error) {
	client, err := s.scyllaClient(ctx, clusterID)
	if err != nil {
		return worker{}, errors.Wrap(err, "get client")
	}
	clusterSession, err := s.clusterSession(ctx, clusterID)
	if err != nil {
		return worker{}, errors.Wrap(err, "get CQL cluster session")
	}

	return worker{
		managerSession: s.session,

		client:         client,
		clusterSession: clusterSession,

		logger: s.logger,

		runInfo: struct{ ClusterID, TaskID, RunID uuid.UUID }{
			ClusterID: clusterID,
			TaskID:    taskID,
			RunID:     runID,
		},
	}, nil
}

// GetProgress aggregates progress for the run of the task.
func (s *Service) GetProgress(ctx context.Context, clusterID, taskID, runID uuid.UUID, _ json.RawMessage) (Progress, error) {
	w, err := s.newWorker(ctx, clusterID, taskID, runID)
	if err != nil {
		return Progress{}, errors.Wrap(err, "new worker")
	}
	pr, err := w.getProgress(ctx)
	if err != nil {
		return Progress{}, errors.Wrap(err, "get progress")
	}
	return pr, nil
}
