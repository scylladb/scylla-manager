// Copyright (C) 2025 ScyllaDB

package tablet

import (
	"context"

	"github.com/pkg/errors"
	"github.com/scylladb/scylla-manager/v3/pkg/scyllaclient"
	"github.com/scylladb/scylla-manager/v3/pkg/util/uuid"
)

type targetWorker struct {
	clusterID uuid.UUID

	client *scyllaclient.Client
}

func (s *Service) newTargetWorker(ctx context.Context, clusterID uuid.UUID) (*targetWorker, error) {
	client, err := s.scyllaClient(ctx, clusterID)
	if err != nil {
		return nil, errors.Wrap(err, "get scylla client")
	}
	return &targetWorker{
		clusterID: clusterID,
		client:    client,
	}, nil
}

func (w *targetWorker) getTarget(ctx context.Context) (Target, error) {
	ni, err := w.client.AnyNodeInfo(ctx)
	if err != nil {
		return Target{}, errors.Wrap(err, "get any node info")
	}
	if ok, err := ni.SupportsTabletRepair(); err != nil {
		return Target{}, errors.Wrap(err, "check tablet repair support")
	} else if !ok {
		return Target{}, errors.Errorf("tablet repair is not supported for ScyllaDB version %s", ni.ScyllaVersion)
	}

	kss, err := w.client.FilteredKeyspaces(ctx, scyllaclient.KeyspaceTypeNonLocal, scyllaclient.ReplicationTablet)
	if err != nil {
		return Target{}, errors.Wrap(err, "get non local tablet keyspaces")
	}

	ksTabs := make(map[string][]string, len(kss))
	for _, ks := range kss {
		tabs, err := w.client.Tables(ctx, ks)
		if err != nil {
			return Target{}, errors.Wrapf(err, "%s: get tables", ks)
		}
		ksTabs[ks] = tabs
	}

	return Target{KsTabs: ksTabs}, nil
}
