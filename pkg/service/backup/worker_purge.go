// Copyright (C) 2017 ScyllaDB

package backup

import (
	"context"

	"github.com/scylladb/mermaid/pkg/util/parallel"
)

func (w *worker) Purge(ctx context.Context, hosts []hostInfo, policy int) (err error) {
	w.Logger.Info(ctx, "Purging old data...")
	defer func() {
		if err != nil {
			w.Logger.Error(ctx, "Purging old data failed see exact errors above")
		} else {
			w.Logger.Info(ctx, "Done purging old data")
		}
	}()

	return hostsInParallel(hosts, parallel.NoLimit, func(h hostInfo) error {
		w.Logger.Info(ctx, "Purging old data on host", "host", h.IP)
		err := w.purgeHost(ctx, h, policy)
		if err != nil {
			w.Logger.Error(ctx, "Purging old data failed on host", "host", h.IP, "error", err)
		} else {
			w.Logger.Info(ctx, "Done purging old data on host", "host", h.IP)
		}
		return err
	})
}

func (w *worker) purgeHost(ctx context.Context, h hostInfo, policy int) error {
	dirs := w.hostSnapshotDirs(h)

	if err := w.Client.DeleteSnapshot(ctx, h.IP, w.SnapshotTag); err != nil {
		w.Logger.Error(ctx, "Failed to delete uploaded snapshot",
			"host", h.IP,
			"tag", w.SnapshotTag,
			"error", err,
		)
	} else {
		w.Logger.Info(ctx, "Deleted uploaded snapshot",
			"host", h.IP,
			"tag", w.SnapshotTag,
		)
	}

	if err := dirsInParallel(dirs, false, func(d snapshotDir) error {
		p := &purger{
			ClusterID: w.ClusterID,
			TaskID:    w.TaskID,
			Keyspace:  d.Keyspace,
			Table:     d.Table,
			Policy:    policy,
			Client:    w.Client,
			Logger:    w.Logger,
		}

		if err := p.purge(ctx, h); err != nil {
			w.Logger.Error(ctx, "Failed to delete remote stale snapshots",
				"host", d.Host,
				"keyspace", p.Keyspace,
				"table", p.Table,
				"location", h.Location,
				"error", err,
			)
			return err
		}
		return nil
	}); err != nil {
		w.Logger.Error(ctx, "Failed to purge snapshots",
			"location", h.Location,
			"error", err,
		)
	}

	return nil
}
