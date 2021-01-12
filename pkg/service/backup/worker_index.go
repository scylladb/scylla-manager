// Copyright (C) 2017 ScyllaDB

package backup

import (
	"context"
	"net/http"
	"path"
	"regexp"
	"time"

	"github.com/pkg/errors"
	"github.com/scylladb/go-set/strset"
	"github.com/scylladb/scylla-manager/pkg/scyllaclient"
	"github.com/scylladb/scylla-manager/pkg/util/timeutc"
)

func (w *worker) Index(ctx context.Context, hosts []hostInfo, limits []DCLimit) (err error) {
	w.Logger.Info(ctx, "Indexing snapshot files...")
	defer func(start time.Time) {
		if err != nil {
			w.Logger.Error(ctx, "Indexing snapshot files failed see exact errors above", "duration", timeutc.Since(start))
		} else {
			w.Logger.Info(ctx, "Done indexing snapshot files", "duration", timeutc.Since(start))
		}
	}(timeutc.Now())

	return inParallelWithLimits(hosts, limits, func(h hostInfo) error {
		w.Logger.Info(ctx, "Indexing snapshot files on host", "host", h.IP)

		dirs, err := w.indexSnapshotDirs(ctx, h)
		if err != nil {
			w.Logger.Error(ctx, "Indexing snapshot files failed on host", "host", h.IP, "error", err)
		} else {
			w.Logger.Info(ctx, "Done indexing snapshot files on host", "host", h.IP)
		}

		w.setSnapshotDirs(h, dirs)

		return err
	})
}

func (w *worker) indexSnapshotDirs(ctx context.Context, h hostInfo) ([]snapshotDir, error) {
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
			localFiles, err := w.Client.RcloneListDir(ctx, h.IP, d.Path, opts)
			if err != nil {
				if scyllaclient.StatusCodeOf(err) == http.StatusNotFound {
					continue
				}
				return nil, errors.Wrap(err, "list table")
			}

			w.Logger.Debug(ctx, "Found snapshot directory",
				"host", h.IP,
				"tag", w.SnapshotTag,
				"keyspace", d.Keyspace,
				"table", d.Table,
				"dir", d.Path,
			)

			var (
				files = make([]fileInfo, 0, len(localFiles))
				size  int64
			)

			mp := path.Join(d.Path, scyllaManifest)
			if err := w.Client.RcloneDeleteFile(ctx, h.IP, mp); err != nil {
				if scyllaclient.StatusCodeOf(err) != http.StatusNotFound {
					w.Logger.Error(ctx, "Failed to delete local manifest file", "error", err)
				}
			}
			sp := path.Join(d.Path, scyllaSchema)
			if err := w.Client.RcloneDeleteFile(ctx, h.IP, sp); err != nil {
				if scyllaclient.StatusCodeOf(err) != http.StatusNotFound {
					w.Logger.Error(ctx, "Failed to delete local schema file", "error", err)
				}
			}

			for _, f := range localFiles {
				// Filter out Scylla manifest and Schema files, they are not needed.
				if f.Name == scyllaManifest || f.Name == scyllaSchema {
					continue
				}

				files = append(files, fileInfo{
					Name: f.Name,
					Size: f.Size,
				})
				size += f.Size
			}
			d.Progress = &RunProgress{
				ClusterID: w.ClusterID,
				TaskID:    w.TaskID,
				RunID:     w.RunID,
				Host:      d.Host,
				Unit:      d.Unit,
				TableName: d.Table,
				Size:      size,
				files:     files,
			}
			w.ResumeUploadProgress(ctx, d.Progress)

			w.onRunProgress(ctx, d.Progress)

			dirs = append(dirs, d)
		}
	}

	if len(dirs) == 0 {
		return nil, errors.New("could not find any files")
	}

	w.Logger.Debug(ctx, "Found snapshot directories", "host", h.IP, "count", len(dirs))
	return dirs, nil
}
