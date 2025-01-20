// Copyright (C) 2025 ScyllaDB

package fastrestore

import (
	"context"
	"encoding/json"

	"github.com/pkg/errors"
	"github.com/scylladb/go-log"
	"github.com/scylladb/gocqlx/v2"

	"github.com/scylladb/scylla-manager/v3/pkg/scyllaclient"
	"github.com/scylladb/scylla-manager/v3/pkg/service/cluster"
	"github.com/scylladb/scylla-manager/v3/pkg/service/configcache"
	"github.com/scylladb/scylla-manager/v3/pkg/util/uuid"
)

// Service for fastnodes restore.
type Service struct {
	session gocqlx.Session

	scyllaClient   scyllaclient.ProviderFunc
	clusterSession cluster.SessionFunc
	configCache    configcache.ConfigCacher
	logger         log.Logger
}

func NewService(session gocqlx.Session, scyllaClient scyllaclient.ProviderFunc, clusterSession cluster.SessionFunc, configCache configcache.ConfigCacher,
	logger log.Logger,
) (*Service, error) {
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

// FastRestore creates and initializes worker performing fast vnode restore with given properties.
func (s *Service) FastRestore(ctx context.Context, clusterID, taskID, runID uuid.UUID, properties json.RawMessage) error {
	s.logger.Info(ctx, "FastRestore",
		"cluster_id", clusterID,
		"task_id", taskID,
		"run_id", runID,
	)

	target, err := s.parseTarget(properties)
	if err != nil {
		return errors.Wrap(err, "parse target")
	}

	s.logger.Info(ctx, "Not yet implemented", "target", target)
	return nil
}

func (s *Service) parseTarget(properties json.RawMessage) (Target, error) {
	target := defaultTarget()
	if err := json.Unmarshal(properties, &target); err != nil {
		return Target{}, errors.Wrap(err, "unmarshal json")
	}
	if err := target.validateProperties(); err != nil {
		return Target{}, errors.Wrap(err, "invalid target")
	}
	return target, nil
}
