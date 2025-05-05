// Copyright (C) 2017 ScyllaDB

package backup

import (
	"context"

	"github.com/pkg/errors"
	"github.com/scylladb/go-log"
	"github.com/scylladb/scylla-manager/backupspec"
	"github.com/scylladb/scylla-manager/v3/pkg/util/parallel"
)

func (w *worker) Purge(ctx context.Context, hosts []hostInfo, retentionMap RetentionMap) (err error) {
	// List manifests in all locations
	manifests, err := listManifestsInAllLocations(ctx, w.Client, hosts, w.ClusterID)
	if err != nil {
		return errors.Wrap(err, "list manifests")
	}
	// Get a list of stale tags
	tags, oldest, err := staleTags(manifests, retentionMap)
	if err != nil {
		return errors.Wrap(err, "get stale snapshot tags")
	}
	// Get a nodeID manifests popping function
	pop := popNodeIDManifestsForLocation(manifests)

	f := func(h hostInfo) error {
		if err := w.Client.DeleteSnapshot(ctx, h.IP, w.SnapshotTag); err != nil {
			w.Logger.Error(ctx, "Failed to delete uploaded snapshot",
				"host", h.IP,
				"snapshot_tag", w.SnapshotTag,
				"error", err,
			)
		} else {
			w.Logger.Info(ctx, "Deleted uploaded snapshot",
				"host", h.IP,
				"snapshot_tag", w.SnapshotTag,
			)
		}

		var (
			nodeID    string
			manifests []*backupspec.ManifestInfo
		)

		p := newPurger(w.Client, h.IP, w.Logger)
		p.OnDelete = func(total, success int) {
			host := p.Host(nodeID)
			if host == "" {
				w.Logger.Debug(ctx, "Missing IP for node ID, not setting purge metrics", "node_id", nodeID)
				return
			}
			w.Metrics.SetPurgeFiles(w.ClusterID, host, total, success)
		}

		fHost := func(nodeID string, manifests []*backupspec.ManifestInfo) error {
			var logger log.Logger
			if nodeID == h.ID {
				logger = w.Logger.With("host", h.IP)
				logger.Info(ctx, "Purging stale snapshots of host")
				defer logger.Info(ctx, "Done purging stale snapshots of host")
			} else {
				logger = w.Logger.With(
					"host", h.IP,
					"node", nodeID,
				)
				logger.Info(ctx, "Purging stale snapshots of node from host")
				defer logger.Info(ctx, "Done purging stale snapshots of node from host")
			}
			p.logger = logger

			_, err := p.PurgeSnapshotTags(ctx, manifests, tags, oldest)
			return err
		}

		for {
			// Get node to purge in the same location, if cannot find any exit
			nodeID, manifests = pop(h)
			if len(manifests) == 0 {
				return nil
			}

			if err := fHost(nodeID, manifests); err != nil {
				return err
			}
		}
	}

	notify := func(h hostInfo, err error) {
		w.Logger.Error(ctx, "Failed to purge stale snapshots",
			"host", h.IP,
			"error", err,
		)
	}

	return hostsInParallel(hosts, parallel.NoLimit, f, notify)
}
