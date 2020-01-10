// Copyright (C) 2017 ScyllaDB

package backup

import (
	"context"
	"path"
	"time"

	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/scylladb/mermaid/pkg/util/parallel"
	"github.com/scylladb/mermaid/pkg/util/retry"
)

func (w *worker) MoveManifests(ctx context.Context, hosts []hostInfo) (err error) {
	w.Logger.Info(ctx, "Moving manifests...")
	defer func() {
		if err != nil {
			w.Logger.Error(ctx, "Moving manifests failed see exact errors above")
		} else {
			w.Logger.Info(ctx, "Done moving manifests")
		}
	}()

	return inParallel(hosts, parallel.NoLimit, func(h hostInfo) error {
		w.Logger.Info(ctx, "Moving manifests on host", "host", h.IP)
		err := w.moveManifestsHost(ctx, h)
		if err != nil {
			w.Logger.Error(ctx, "Moving manifests failed on host", "host", h.IP, "error", err)
		} else {
			w.Logger.Info(ctx, "Done moving manifests on host", "host", h.IP)
		}
		return err
	})
}

func (w *worker) moveManifestsHost(ctx context.Context, h hostInfo) error {
	dirs := w.hostSnapshotDirs(h)

	for _, d := range dirs {
		w.Logger.Info(ctx, "Moving manifest",
			"host", h.IP,
			"keyspace", d.Keyspace,
			"table", d.Table,
			"location", h.Location,
		)
		var (
			manifestDst = h.Location.RemotePath(w.remoteManifestFile(h, d))
			manifestSrc = h.Location.RemotePath(path.Join(w.remoteSSTableDir(h, d), manifest))
		)
		if err := w.moveManifestFile(ctx, manifestDst, manifestSrc, d); err != nil {
			return errors.Wrapf(err, "move %q to %q", manifestSrc, manifestDst)
		}
	}

	return nil
}

func (w *worker) moveManifestFile(ctx context.Context, manifestDst, manifestSrc string, d snapshotDir) error {
	b := moveManifestBackoff()
	notify := func(err error, wait time.Duration) {
		w.Logger.Info(ctx, "Moving manifest failed, retrying", "host", d.Host, "from", manifestSrc, "to", manifestDst, "error", err, "wait", wait)

		backupMoveManifestRetries.With(prometheus.Labels{
			"cluster":  w.ClusterName,
			"task":     w.TaskID.String(),
			"host":     d.Host,
			"keyspace": d.Keyspace,
		}).Inc()
	}

	return retry.WithNotify(ctx, func() error {
		return w.Client.RcloneMoveFile(ctx, d.Host, manifestDst, manifestSrc)
	}, b, notify)
}

func moveManifestBackoff() retry.Backoff {
	initialInterval := 5 * time.Second
	maxElapsedTime := 10 * time.Minute
	maxInterval := 1 * time.Minute
	multiplier := 1.5
	randomFactor := 0.5
	return retry.NewExponentialBackoff(initialInterval, maxElapsedTime, maxInterval, multiplier, randomFactor)
}
