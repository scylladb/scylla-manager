// Copyright (C) 2017 ScyllaDB

package backup

import (
	"context"

	"github.com/scylladb/mermaid/internal/parallel"
)

func (w *worker) Purge(ctx context.Context, hosts []hostInfo, policy int) (err error) {
	w.Logger.Info(ctx, "Starting purge procedure")
	defer func() {
		if err != nil {
			w.Logger.Error(ctx, "Purge procedure completed with error(s) see exact errors above")
		} else {
			w.Logger.Info(ctx, "Purge procedure completed")
		}
	}()

	return inParallel(hosts, parallel.NoLimit, func(h hostInfo) error {
		w.Logger.Info(ctx, "Executing purge procedure on host", "host", h.IP)
		err := w.purgeHost(ctx, h, policy)
		if err != nil {
			w.Logger.Error(ctx, "Purge procedure failed on host", "host", h.IP, "error", err)
		} else {
			w.Logger.Info(ctx, "Done executing purge procedure on host", "host", h.IP)
		}
		return err
	})
}

func (w *worker) purgeHost(ctx context.Context, h hostInfo, policy int) error {
	dirs := w.hostSnapshotDirs(h)

	for _, d := range dirs {
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
		}
	}

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

	return nil
}
