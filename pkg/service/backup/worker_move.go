// Copyright (C) 2017 ScyllaDB

package backup

import (
	"context"
	"path"

	"github.com/scylladb/mermaid/pkg/util/parallel"
)

func (w *worker) MoveManifests(ctx context.Context, hosts []hostInfo) (err error) {
	w.Logger.Info(ctx, "Starting move manifests procedure")
	defer func() {
		if err != nil {
			w.Logger.Error(ctx, "Move manifests procedure completed with error(s) see exact errors above")
		} else {
			w.Logger.Info(ctx, "Move manifests procedure completed")
		}
	}()

	return inParallel(hosts, parallel.NoLimit, func(h hostInfo) error {
		w.Logger.Info(ctx, "Executing move manifests procedure on host", "host", h.IP)
		err := w.moveManifestsHost(ctx, h)
		if err != nil {
			w.Logger.Error(ctx, "Move manifests procedure failed on host", "host", h.IP, "error", err)
		} else {
			w.Logger.Info(ctx, "Done executing move manifests on host", "host", h.IP)
		}
		return err
	})
}

func (w *worker) moveManifestsHost(ctx context.Context, h hostInfo) error {
	dirs := w.hostSnapshotDirs(h)

	for _, d := range dirs {
		w.Logger.Info(ctx, "Moving table manifest",
			"host", h.IP,
			"keyspace", d.Keyspace,
			"table", d.Table,
			"location", h.Location,
		)
		var (
			manifestDst = h.Location.RemotePath(w.remoteManifestFile(h, d))
			manifestSrc = h.Location.RemotePath(path.Join(w.remoteSSTableDir(h, d), manifest))
		)

		if err := w.Client.RcloneMoveFile(ctx, d.Host, manifestDst, manifestSrc); err != nil {
			return err
		}
	}

	return nil
}
