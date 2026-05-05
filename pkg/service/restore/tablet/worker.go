// Copyright (C) 2026 ScyllaDB

package tablet

import (
	"context"
	"net/netip"

	"github.com/pkg/errors"
	"github.com/scylladb/go-log"
	"github.com/scylladb/scylla-manager/backupspec"
	"github.com/scylladb/scylla-manager/v3/pkg/scyllaclient"
	"github.com/scylladb/scylla-manager/v3/pkg/service/configcache"
	cqlTable "github.com/scylladb/scylla-manager/v3/pkg/table"
	"golang.org/x/sync/errgroup"
)

// Workload represents all data needed for tablet aware restore purposes.
type Workload map[cqlTable.CQLTable]TableMeta

// TableMeta represents data needed for restoring table with tablet aware restore.
type TableMeta struct {
	Table           cqlTable.CQLTable
	SnapshotTag     string
	RemoteManifests map[backupspec.Location][]string
	FileCnt         int64
	Size            int64
}

// RestoreWorker is a set of tools responsible for
// restoring tables with tablet aware restore.
type RestoreWorker struct {
	logger             log.Logger
	client             *scyllaclient.Client
	nodeConfig         map[netip.Addr]configcache.NodeConfig
	longPollingSeconds int
}

// NewRestoreWorker is a constructor for RestoreWorker.
func NewRestoreWorker(logger log.Logger, client *scyllaclient.Client, nodeConfig map[netip.Addr]configcache.NodeConfig, longPollingSeconds int) *RestoreWorker {
	return &RestoreWorker{
		logger:             logger.Named("tablet_restore"),
		client:             client,
		nodeConfig:         nodeConfig,
		longPollingSeconds: longPollingSeconds,
	}
}

// Restore given tables with tablet aware restore based on provided workload.
func (w *RestoreWorker) Restore(ctx context.Context, tables Workload) error {
	w.logger.Info(ctx, "Started tablet aware restore")
	defer w.logger.Info(ctx, "Finished tablet aware restore")

	eg, egCtx := errgroup.WithContext(ctx)
	for _, tm := range tables {
		eg.Go(func() error {
			return errors.Wrapf(w.restoreTable(egCtx, tm), "tablet aware restore table %s.%s", tm.Table.Keyspace, tm.Table.Name)
		})
	}
	return eg.Wait()
}

// restoreTable by scheduling tablet aware restore task and waiting for its completion.
func (w *RestoreWorker) restoreTable(ctx context.Context, tm TableMeta) error {
	var anyHost string
	for ip := range w.nodeConfig {
		anyHost = ip.String()
	}

	locations := make([]scyllaclient.TabletRestoreLocation, 0, len(tm.RemoteManifests))
	for loc, manifests := range tm.RemoteManifests {
		nodeCfg, err := w.nodeConfigForDC(loc.DC)
		if err != nil {
			return errors.Wrapf(err, "get node config for location %s", loc)
		}
		endpoint, err := nodeCfg.ScyllaObjectStorageEndpoint(loc.Provider)
		if err != nil {
			return errors.Wrapf(err, "get object storage endpoint for location %s", loc)
		}
		locations = append(locations, scyllaclient.TabletRestoreLocation{
			Endpoint:   endpoint,
			Bucket:     loc.Path,
			Datacenter: loc.DC,
			Manifests:  manifests,
		})
	}

	w.logger.Info(ctx, "Started table tablet aware restore",
		"keyspace", tm.Table.Keyspace,
		"table", tm.Table.Name,
		"host", anyHost,
		"locations", locations,
	)
	defer w.logger.Info(ctx, "Finished table tablet aware restore", "keyspace", tm.Table.Keyspace, "table", tm.Table.Name)
	id, err := w.client.TabletRestore(ctx, anyHost, tm.Table.Keyspace, tm.Table.Name, tm.SnapshotTag, locations)
	if err != nil {
		return errors.Wrapf(err, "schedule tablet aware restore on node %s", anyHost)
	}

	if err := w.waitTask(ctx, anyHost, id); err != nil {
		return errors.Wrapf(err, "wait for tablet aware restore task %s on node %s", id, anyHost)
	}
	return nil
}

func (w *RestoreWorker) waitTask(ctx context.Context, host, id string) error {
	for {
		if ctx.Err() != nil {
			w.abortTask(host, id)
			return ctx.Err()
		}

		task, err := w.client.ScyllaWaitTask(ctx, host, id, int64(w.longPollingSeconds))
		if err != nil {
			w.abortTask(host, id)
			return errors.Wrap(err, "long poll task status")
		}

		switch scyllaclient.ScyllaTaskState(task.State) {
		case scyllaclient.ScyllaTaskStateFailed:
			return errors.New("scylla task failed: " + task.Error)
		case scyllaclient.ScyllaTaskStateDone:
			return nil
		}
	}
}

func (w *RestoreWorker) abortTask(host, id string) {
	if err := w.client.ScyllaAbortTask(context.Background(), host, id); err != nil {
		w.logger.Error(context.Background(), "Failed to abort task",
			"host", host,
			"id", id,
			"error", err,
		)
	}
}

// nodeConfigForDC returns config of a node from the given datacenter.
func (w *RestoreWorker) nodeConfigForDC(dc string) (configcache.NodeConfig, error) {
	for _, nc := range w.nodeConfig {
		if nc.Datacenter == dc {
			return nc, nil
		}
	}
	return configcache.NodeConfig{}, errors.Errorf("no node found in datacenter %s", dc)
}
