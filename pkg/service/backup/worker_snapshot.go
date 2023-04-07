// Copyright (C) 2017 ScyllaDB

package backup

import (
	"context"
	"time"

	"github.com/pkg/errors"
	. "github.com/scylladb/scylla-manager/v3/pkg/service/backup/backupspec"
	"github.com/scylladb/scylla-manager/v3/pkg/util/timeutc"
)

func (w *worker) Snapshot(ctx context.Context, hosts []hostInfo, limits []DCLimit) (err error) {
	w.Logger.Info(ctx, "Taking snapshots...")
	defer func(start time.Time) {
		if err != nil {
			w.Logger.Error(ctx, "Taking snapshots failed see exact errors above", "duration", timeutc.Since(start))
		} else {
			w.Logger.Info(ctx, "Done taking snapshots", "duration", timeutc.Since(start))
		}
	}(timeutc.Now())

	return inParallelWithLimits(hosts, limits, func(h hostInfo) error {
		w.Logger.Info(ctx, "Taking snapshots on host", "host", h.IP)
		err := w.snapshotHost(ctx, h)
		if err != nil {
			w.Logger.Error(ctx, "Taking snapshots failed on host", "host", h.IP, "error", err)
		} else {
			w.Logger.Info(ctx, "Done taking snapshots on host", "host", h.IP)
		}
		return err
	})
}

func (w *worker) snapshotHost(ctx context.Context, h hostInfo) error {
	if err := w.deleteOldSnapshots(ctx, h); err != nil {
		return errors.Wrap(err, "delete old snapshots")
	}
	if err := w.checkAvailableDiskSpace(ctx, h); err != nil {
		return err
	}
	return w.takeSnapshot(ctx, h)
}

func (w *workerTools) checkAvailableDiskSpace(ctx context.Context, h hostInfo) error {
	freePercent, err := w.diskFreePercent(ctx, h)
	if err != nil {
		return err
	}
	w.Logger.Info(ctx, "Available disk space", "host", h.IP, "percent", freePercent)
	if freePercent < w.Config.DiskSpaceFreeMinPercent {
		return errors.New("not enough disk space")
	}
	return nil
}

func (w *workerTools) diskFreePercent(ctx context.Context, h hostInfo) (int, error) {
	du, err := w.Client.RcloneDiskUsage(ctx, h.IP, DataDir)
	if err != nil {
		return 0, err
	}
	return int(100 * (float64(du.Free) / float64(du.Total))), nil
}

func (w *worker) takeSnapshot(ctx context.Context, h hostInfo) error {
	// Mark units as pending snapshot
	for _, u := range w.Units {
		w.Metrics.SetSnapshot(w.ClusterID, u.Keyspace, h.IP, false)
	}

	// Taking a snapshot can be a costly operation.
	// To optimize that we randomise order of taking snapshots on different nodes.
	for _, i := range unitsPerm(w.Units) {
		u := w.Units[i]

		w.Logger.Info(ctx, "Taking snapshot", "host", h.IP, "keyspace", u.Keyspace, "snapshot_tag", w.SnapshotTag)
		var tables []string
		if !u.AllTables {
			tables = u.Tables
		}
		if err := w.Client.TakeSnapshot(ctx, h.IP, w.SnapshotTag, u.Keyspace, tables...); err != nil {
			return errors.Wrapf(err, "keyspace %s: snapshot", u.Keyspace)
		}
		w.Metrics.SetSnapshot(w.ClusterID, u.Keyspace, h.IP, true)
	}
	return nil
}

func (w *worker) deleteOldSnapshots(ctx context.Context, h hostInfo) error {
	tags, err := w.Client.Snapshots(ctx, h.IP)
	if err != nil {
		return err
	}

	var deleted []string
	defer func() {
		if len(deleted) > 0 {
			w.Logger.Info(ctx, "Deleted old snapshots",
				"host", h.IP,
				"tags", deleted,
			)
		} else {
			w.Logger.Info(ctx, "No old snapshots to delete", "host", h.IP)
		}
	}()

	for _, t := range tags {
		if IsSnapshotTag(t) && t != w.SnapshotTag {
			if err := w.Client.DeleteSnapshot(ctx, h.IP, t); err != nil {
				return err
			}
			deleted = append(deleted, t)
		}
	}

	return nil
}
