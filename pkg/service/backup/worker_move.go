// Copyright (C) 2017 ScyllaDB

package backup

import (
	"context"
	"path"
	"time"

	"github.com/pkg/errors"
	"github.com/scylladb/mermaid/pkg/util/parallel"
	"go.uber.org/multierr"
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
		id, err := w.Client.RcloneMoveFile(ctx, d.Host, manifestDst, manifestSrc)
		if err != nil {
			return err
		}
		if err := w.waitMove(ctx, id, d); err != nil {
			return err
		}
	}

	return nil
}

func (w *worker) waitMove(ctx context.Context, id int64, d snapshotDir) (err error) {
	defer func() {
		err = multierr.Combine(
			err,
			w.clearJobStats(ctx, id, d.Host),
		)
	}()

	t := time.NewTicker(w.Config.PollInterval)
	defer t.Stop()

	for {
		select {
		case <-ctx.Done():
			err := w.Client.RcloneJobStop(context.Background(), d.Host, id)
			if err != nil {
				w.Logger.Error(ctx, "Failed to stop rclone job",
					"error", err,
					"host", d.Host,
					"unit", d.Unit,
					"job_id", id,
					"table", d.Table,
				)
			}
			return ctx.Err()
		case <-t.C:
			status, err := w.getJobStatus(ctx, id, d)
			switch status {
			case jobError:
				return err
			case jobNotFound:
				return errors.Errorf("job not found (%d)", id)
			case jobSuccess:
				return nil
			case jobRunning:
				// Continue
			}
		}
	}
}
