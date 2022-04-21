// Copyright (C) 2017 ScyllaDB

package backup

import (
	"context"
	"sync"
	"time"

	"github.com/pkg/errors"
	"github.com/scylladb/go-log"
	. "github.com/scylladb/scylla-manager/v3/pkg/service/backup/backupspec"
	"github.com/scylladb/scylla-manager/v3/pkg/util/timeutc"
	"go.uber.org/multierr"
)

func (w *worker) Purge(ctx context.Context, hosts []hostInfo, purgeParallel int, retentionMap RetentionMap) (err error) {
	w.Logger.Info(ctx, "Purging stale snapshots...")
	defer func(start time.Time) {
		if err != nil {
			w.Logger.Error(ctx, "Purging stale snapshots failed see exact errors above", "duration", timeutc.Since(start))
		} else {
			w.Logger.Info(ctx, "Done purging stale snapshots", "duration", timeutc.Since(start))
		}
	}(timeutc.Now())

	// List manifests in all locations
	manifests, err := listManifestsInAllLocations(ctx, w.Client, hosts, w.ClusterID)
	if err != nil {
		return errors.Wrap(err, "list manifests")
	}
	// Get a list of stale tags
	tags := staleTags(manifests, retentionMap)
	// Get a nodeID manifests popping function
	pop := popNodeIDManifestsForLocation(manifests)

	var (
		purgeErr error
		errMutex sync.Mutex
		wg       sync.WaitGroup
	)

	err = hostsInParallel(hosts, purgeParallel, func(h hostInfo) error {
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
			manifests []*ManifestInfo
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

		f := func(nodeID string, manifests []*ManifestInfo) error {
			var logger log.Logger
			if nodeID == h.ID {
				logger := w.Logger.With("host", h.IP)
				logger.Info(ctx, "Purging stale snapshots of host")
				defer logger.Info(ctx, "Done purging stale snapshots of host")
			} else {
				logger := w.Logger.With(
					"host", h.IP,
					"node", nodeID,
				)
				logger.Info(ctx, "Purging stale snapshots of node from host")
				defer logger.Info(ctx, "Done purging stale snapshots of node from host")
			}
			p.logger = logger

			files, err := p.CollectPurgeableFiles(ctx, manifests, tags)
			if err != nil {
				return err
			}

			wg.Add(1)
			go func() {
				defer wg.Done()
				if _, err := p.PurgeFiles(ctx, manifests, tags, files); err != nil {
					errMutex.Lock()
					defer errMutex.Unlock()
					purgeErr = multierr.Append(purgeErr, err)
				}
			}()

			return nil
		}

		for {
			// Get node to purge in the same location, if cannot find any exit
			nodeID, manifests = pop(h)
			if len(manifests) == 0 {
				return nil
			}

			if err := f(nodeID, manifests); err != nil {
				return err
			}
		}
	})

	wg.Wait()

	err = multierr.Append(err, purgeErr)
	return err
}
