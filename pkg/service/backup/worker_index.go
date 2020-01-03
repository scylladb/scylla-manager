// Copyright (C) 2017 ScyllaDB

package backup

import (
	"context"
	"net/http"
	"path"
	"regexp"

	"github.com/pkg/errors"
	"github.com/scylladb/go-set/strset"
	"github.com/scylladb/mermaid/pkg/scyllaclient"
)

func (w *worker) Index(ctx context.Context, hosts []hostInfo, limits []DCLimit) (err error) {
	w.Logger.Info(ctx, "Starting index procedure")
	defer func() {
		if err != nil {
			w.Logger.Error(ctx, "Index procedure completed with error(s) see exact errors above")
		} else {
			w.Logger.Info(ctx, "Index procedure completed")
		}
	}()

	return inParallelWithLimits(hosts, limits, func(h hostInfo) error {
		w.Logger.Info(ctx, "Executing index procedure on host", "host", h.IP)

		dirs, err := w.indexSnapshotDirs(ctx, h)
		if err != nil {
			w.Logger.Error(ctx, "Index procedure failed on host", "host", h.IP, "error", err)
		} else {
			w.Logger.Info(ctx, "Done executing index procedure on host", "host", h.IP)
		}
		w.setHostSnapshotDirs(h, dirs)

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
			files, err := w.Client.RcloneListDir(ctx, h.IP, d.Path, opts)
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
				fileNames []string
				size      int64
			)
			for _, f := range files {
				fileNames = append(fileNames, f.Name)
				size += f.Size
			}
			d.Progress = &RunProgress{
				ClusterID: w.ClusterID,
				TaskID:    w.TaskID,
				RunID:     w.RunID,
				Host:      d.Host,
				Unit:      d.Unit,
				TableName: d.Table,
				Files:     fileNames,
				Size:      size,
			}
			w.onRunProgress(ctx, d.Progress)

			dirs = append(dirs, d)
		}
	}
	w.Logger.Debug(ctx, "Found snapshot directories", "host", h.IP, "count", len(dirs))

	return dirs, nil
}
