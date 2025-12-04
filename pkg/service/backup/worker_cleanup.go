// Copyright (C) 2025 ScyllaDB

package backup

import (
	"context"

	"github.com/pkg/errors"
	"github.com/scylladb/go-log"
	"github.com/scylladb/scylla-manager/backupspec"
	"github.com/scylladb/scylla-manager/v3/pkg/scyllaclient"
	"github.com/scylladb/scylla-manager/v3/pkg/util/uuid"
	"golang.org/x/sync/errgroup"
)

type cleanupWorker struct {
	clusterID uuid.UUID

	logger log.Logger
	client *scyllaclient.Client

	hosts []string
}

func (s *Service) newCleanupWorker(ctx context.Context, clusterID uuid.UUID) (*cleanupWorker, error) {
	client, err := s.scyllaClient(ctx, clusterID)
	if err != nil {
		return nil, errors.Wrap(err, "get scylla client")
	}
	status, err := client.Status(ctx)
	if err != nil {
		return nil, errors.Wrap(err, "get status")
	}
	return &cleanupWorker{
		clusterID: clusterID,
		logger:    s.logger.Named("cleanup_worker"),
		client:    client,
		hosts:     status.Up().Hosts(),
	}, nil
}

func (w *cleanupWorker) cleanupAll(ctx context.Context) error {
	eg, egCtx := errgroup.WithContext(ctx)
	for _, host := range w.hosts {
		eg.Go(func() error {
			return errors.Wrapf(w.cleanupHost(egCtx, host), "host %s", host)
		})
	}
	return eg.Wait()
}

func (w *cleanupWorker) cleanupHost(ctx context.Context, host string) error {
	tags, err := w.client.Snapshots(ctx, host)
	if err != nil {
		return errors.Wrap(err, "list snapshots")
	}
	w.logger.Info(ctx, "Listed snapshots", "host", host, "tags", tags)

	for _, tag := range tags {
		if backupspec.IsSnapshotTag(tag) {
			w.logger.Info(ctx, "Delete snapshot", "host", host, "tag", tag)
			if err := w.client.DeleteSnapshot(ctx, host, tag); err != nil {
				return errors.Wrapf(err, "delete snapshot %s", tag)
			}
		}
	}
	return nil
}
