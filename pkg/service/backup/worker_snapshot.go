// Copyright (C) 2017 ScyllaDB

package backup

import (
	"context"
	"math/rand"
	"net/http"
	"path"
	"regexp"
	"strings"

	"github.com/pkg/errors"
	"github.com/scylladb/go-set/strset"
	"github.com/scylladb/mermaid/pkg/scyllaclient"
)

func (w *worker) Snapshot(ctx context.Context, hosts []hostInfo, limits []DCLimit) (err error) {
	w.Logger.Info(ctx, "Starting snapshot procedure")
	defer func() {
		if err != nil {
			w.Logger.Error(ctx, "Snapshot procedure completed with error(s) see exact errors above")
		} else {
			w.Logger.Info(ctx, "Snapshot procedure completed")
		}
	}()

	return inParallelWithLimits(hosts, limits, func(h hostInfo) error {
		w.Logger.Info(ctx, "Executing snapshot procedure on host", "host", h.IP)
		err := w.snapshotHost(ctx, h)
		if err != nil {
			w.Logger.Error(ctx, "Snapshot procedure failed on host", "host", h.IP, "error", err)
		} else {
			w.Logger.Info(ctx, "Done executing snapshot procedure on host", "host", h.IP)
		}
		return err
	})
}

func (w *worker) snapshotHost(ctx context.Context, h hostInfo) error {
	if err := w.checkAvailableDiskSpace(ctx, h); err != nil {
		return errors.Wrap(err, "disk space check")
	}
	if err := w.takeSnapshot(ctx, h); err != nil {
		return errors.Wrap(err, "take snapshot")
	}
	if err := w.deleteOldSnapshots(ctx, h); err != nil {
		// Not a fatal error we can continue, just log the error
		w.Logger.Error(ctx, "Failed to delete old snapshots", "error", err)
	}

	dirs, err := w.findSnapshotDirs(ctx, h)
	if err != nil {
		return errors.Wrap(err, "list snapshot dirs")
	}
	w.setHostSnapshotDirs(h, dirs)

	return nil
}

func (w *worker) checkAvailableDiskSpace(ctx context.Context, h hostInfo) error {
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

func (w *worker) diskFreePercent(ctx context.Context, h hostInfo) (int, error) {
	du, err := w.Client.RcloneDiskUsage(ctx, h.IP, dataDir)
	if err != nil {
		return 0, err
	}
	return int(100 * (float64(du.Free) / float64(du.Total))), nil
}

func (w *worker) takeSnapshot(ctx context.Context, h hostInfo) error {
	// Double check that the snapshot does not exist on host.
	units, err := w.Client.SnapshotDetails(ctx, h.IP, w.SnapshotTag)
	if err != nil {
		return errors.Wrapf(err, "check snapshot exists")
	}
	if len(units) != 0 {
		w.Logger.Debug(ctx, "Snapshot already exists",
			"host", h.IP,
			"tag", w.SnapshotTag,
			"units", units,
		)
		return errors.Errorf("snapshot %s already exists", w.SnapshotTag)
	}

	// Taking a snapshot can be a costly operation. To optimise that clusterwise
	// we randomise order of taking snapshots (kesypace and tables) on different
	// hosts. This jitter prevents form flushing a single table on all nodes
	// at the same time.
	for _, uPos := range rand.Perm(len(w.Units)) {
		u := w.Units[uPos]

		for _, tPos := range rand.Perm(len(u.Tables)) {
			t := u.Tables[tPos]

			w.Logger.Info(ctx, "Taking snapshot",
				"host", h.IP,
				"keyspace", u.Keyspace,
				"table", t,
			)

			if err := w.Client.TakeSnapshot(ctx, h.IP, w.SnapshotTag, u.Keyspace, t); err != nil {
				w.Logger.Debug(ctx, "Snapshot error",
					"keyspace", u.Keyspace,
					"table", u.Tables[tPos],
					"tag", w.SnapshotTag,
					"error", err,
				)

				// Ignore snapshot already exists error.
				// We checked that it did not exist before...
				// It must have taken long time to execute and failed on retry.
				if !strings.Contains(err.Error(), "snapshot "+w.SnapshotTag+" already exists") {
					return errors.Wrapf(err, "keyspace %s: snapshot failed", u.Keyspace)
				}
			}
		}
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
		if isSnapshotTag(t) && t != w.SnapshotTag {
			if err := w.Client.DeleteSnapshot(ctx, h.IP, t); err != nil {
				return err
			}
			deleted = append(deleted, t)
		}
	}

	return nil
}

func (w *worker) findSnapshotDirs(ctx context.Context, h hostInfo) ([]snapshotDir, error) {
	var dirs []snapshotDir

	r := regexp.MustCompile("^([A-Za-z0-9_]+)-([a-f0-9]{32})$")

	for i, u := range w.Units {
		w.Logger.Debug(ctx, "Finding table snapshot directories",
			"host", h.IP,
			"tag", w.SnapshotTag,
			"keyspace", u.Keyspace,
		)

		baseDir := keyspaceDir(u.Keyspace)

		tables, err := w.Client.RcloneListDir(ctx, h.IP, baseDir, nil)
		if err != nil {
			return nil, errors.Wrap(err, "list keyspace")
		}

		filter := strset.New(u.Tables...)

		for _, t := range tables {
			m := r.FindStringSubmatch(t.Path)
			if m == nil {
				continue
			}

			d := snapshotDir{
				Host:     h.IP,
				Unit:     int64(i),
				Path:     path.Join(baseDir, t.Path, "snapshots", w.SnapshotTag),
				Keyspace: u.Keyspace,
				Table:    m[1],
				Version:  m[2],
			}

			if !filter.IsEmpty() && !filter.Has(d.Table) {
				continue
			}

			opts := &scyllaclient.RcloneListDirOpts{
				FilesOnly: true,
			}
			files, err := w.Client.RcloneListDir(ctx, h.IP, d.Path, opts)
			if err != nil {
				if scyllaclient.StatusCodeOf(err) == http.StatusNotFound {
					continue
				}
				return nil, errors.Wrap(err, "list table")
			}

			w.Logger.Debug(ctx, "Found snapshot table directory",
				"host", h.IP,
				"tag", w.SnapshotTag,
				"keyspace", d.Keyspace,
				"table", d.Table,
				"dir", d.Path,
			)

			for _, f := range files {
				if f.Name == manifest {
					// Manifest is metadata so we are excluding it from the
					// total progress of the upload.
					continue
				}
				p := &RunProgress{
					ClusterID: w.ClusterID,
					TaskID:    w.TaskID,
					RunID:     w.RunID,
					Host:      d.Host,
					Unit:      d.Unit,
					TableName: d.Table,
					FileName:  f.Name,
					Size:      f.Size,
				}
				d.Progress = append(d.Progress, p)
				w.onRunProgress(ctx, p)
			}

			dirs = append(dirs, d)
		}
	}

	return dirs, nil
}
