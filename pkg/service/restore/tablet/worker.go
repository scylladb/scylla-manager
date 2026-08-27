// Copyright (C) 2026 ScyllaDB

package tablet

import (
	"context"
	"net/netip"
	"sync"

	"github.com/pkg/errors"
	"github.com/scylladb/go-log"
	"github.com/scylladb/scylla-manager/v3/pkg/scyllaclient"
	"github.com/scylladb/scylla-manager/v3/pkg/service/configcache"
	"golang.org/x/sync/errgroup"
)

// RestoreWorker is a set of tools responsible for
// restoring tables with tablet aware restore.
type RestoreWorker struct {
	logger             log.Logger
	client             *scyllaclient.Client
	nodeConfig         map[netip.Addr]configcache.NodeConfig
	hostPicker         *dcHostPicker
	longPollingSeconds int
}

// NewRestoreWorker is a constructor for RestoreWorker.
func NewRestoreWorker(logger log.Logger, client *scyllaclient.Client, nodeConfig map[netip.Addr]configcache.NodeConfig, longPollingSeconds int) *RestoreWorker {
	return &RestoreWorker{
		logger:             logger.Named("tablet_restore"),
		client:             client,
		nodeConfig:         nodeConfig,
		hostPicker:         newDCHostPicker(nodeConfig),
		longPollingSeconds: longPollingSeconds,
	}
}

// Restore given tables with tablet aware restore based on provided workload.
func (w *RestoreWorker) Restore(ctx context.Context, tables Workload) error {
	w.logger.Info(ctx, "Started tablet aware restore")
	defer w.logger.Info(ctx, "Finished tablet aware restore")

	// We don't need to limit concurrency because of scylla,
	// because it handles parallel tablet aware restores according
	// to its tablet scheduler. We want to set sane limits to avoid
	// exhausting too many resources on SM and SM agent sides.
	clusterShards := 0
	for _, nc := range w.nodeConfig {
		clusterShards += int(nc.CPUCount)
	}
	limit := max(100, clusterShards)
	limit = min(1000, limit)
	w.logger.Info(ctx, "Calculated concurrency limit", "limit", limit)

	eg := errgroup.Group{}
	eg.SetLimit(limit)
	for _, tm := range tables {
		eg.Go(func() error {
			return errors.Wrapf(w.restoreTable(ctx, tm), "tablet aware restore table %s.%s", tm.Table.Keyspace, tm.Table.Name)
		})
	}
	return eg.Wait()
}

// restoreTable by scheduling tablet aware restore task and waiting for its completion.
func (w *RestoreWorker) restoreTable(ctx context.Context, tm TableMeta) error {
	// Spread requests evenly across hosts in target DCs
	var host string
	locations := make([]scyllaclient.TabletRestoreLocation, 0, len(tm.Datacenters))
	for _, dcMeta := range tm.Datacenters {
		nodeIP, err := w.hostPicker.pick(dcMeta.DC)
		if err != nil {
			return errors.Wrapf(err, "get node config for dc %s", dcMeta.DC)
		}
		nodeCfg := w.nodeConfig[nodeIP]
		endpoint, err := nodeCfg.ScyllaObjectStorageEndpoint(dcMeta.Provider)
		if err != nil {
			return errors.Wrapf(err, "get object storage endpoint for dc %s", dcMeta.DC)
		}
		host = nodeIP.String()
		locations = append(locations, scyllaclient.TabletRestoreLocation{
			Endpoint: endpoint,
			Bucket:   dcMeta.Bucket,
			// Note that DcMeta contains source DC name.
			// When scheduling tablet aware restore, we should use target DC names.
			// They might be different when --dc-mapping is used.
			// For now, tablet aware restore does not support --dc-mapping changing
			// DC names, so no change needs to be applied, but when it happens,
			// we need to remember to apply it here as well.
			Datacenter: dcMeta.DC,
			Manifests:  dcMeta.RemoteManifests,
		})
	}

	w.logger.Info(ctx, "Started table tablet aware restore",
		"keyspace", tm.Table.Keyspace,
		"table", tm.Table.Name,
		"host", host,
		"locations", locations,
	)
	defer w.logger.Info(ctx, "Finished table tablet aware restore", "keyspace", tm.Table.Keyspace, "table", tm.Table.Name)
	id, err := w.client.TabletRestore(ctx, host, tm.Table.Keyspace, tm.Table.Name, tm.SnapshotTag, locations)
	if err != nil {
		return errors.Wrapf(err, "schedule tablet aware restore on node %s", host)
	}

	if err := w.waitTask(ctx, host, id); err != nil {
		return errors.Wrapf(err, "wait for tablet aware restore task %s on node %s", id, host)
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

// dcHostPicker picks hosts from requested datacenter using round-robin.
// It's safe for concurrent use.
type dcHostPicker struct {
	mu      sync.Mutex
	dcHosts map[string][]netip.Addr
	dcIdx   map[string]int
}

func newDCHostPicker(nodeConfig map[netip.Addr]configcache.NodeConfig) *dcHostPicker {
	dcHosts := make(map[string][]netip.Addr)
	for ip, nc := range nodeConfig {
		dcHosts[nc.Datacenter] = append(dcHosts[nc.Datacenter], ip)
	}
	return &dcHostPicker{
		dcHosts: dcHosts,
		dcIdx:   make(map[string]int),
	}
}

// pick returns IP of the next host from the given datacenter.
func (p *dcHostPicker) pick(dc string) (netip.Addr, error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	hosts := p.dcHosts[dc]
	if len(hosts) == 0 {
		return netip.Addr{}, errors.Errorf("no node found in datacenter %s", dc)
	}
	ip := hosts[p.dcIdx[dc]%len(hosts)]
	p.dcIdx[dc]++
	return ip, nil
}
