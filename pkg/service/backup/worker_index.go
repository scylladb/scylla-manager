// Copyright (C) 2017 ScyllaDB

package backup

import (
	"context"
	"net/http"
	"path"
	"regexp"
	"sort"
	"time"

	"github.com/pkg/errors"
	"github.com/scylladb/go-set/strset"
	"github.com/scylladb/scylla-manager/backupspec"
	"github.com/scylladb/scylla-manager/v3/pkg/scyllaclient"
)

func (w *worker) Index(ctx context.Context, hosts []hostInfo, limits []DCLimit) (err error) {
	f := func(h hostInfo) error {
		w.Logger.Info(ctx, "Indexing snapshot files on host", "host", h.IP)

		dirs, err := w.indexSnapshotDirs(ctx, h)
		if err == nil {
			w.Logger.Info(ctx, "Done indexing snapshot files on host", "host", h.IP)
		}

		w.setSnapshotDirs(h, dirs)
		return err
	}

	notify := func(h hostInfo, err error) {
		w.Logger.Error(ctx, "Indexing snapshot files failed on host", "host", h.IP, "error", err)
	}

	return inParallelWithLimits(hosts, limits, f, notify)
}

func (w *worker) indexSnapshotDirs(ctx context.Context, h hostInfo) ([]snapshotDir, error) {
	var dirs []snapshotDir

	nftt := w.newFilesTimeThreshold()
	// Include '-' and '.' as valid character because of alternator tables
	r := regexp.MustCompile("^([A-Za-z0-9_.-]+)-([a-f0-9]{32})$")

	for i, u := range w.Units {
		w.Logger.Debug(ctx, "Finding table snapshot directories",
			"host", h.IP,
			"snapshot_tag", w.SnapshotTag,
			"keyspace", u.Keyspace,
			"new_files_time_threshold", nftt,
		)

		baseDir := backupspec.KeyspaceDir(u.Keyspace)

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

			w.Logger.Debug(ctx, "Found snapshot directory",
				"host", h.IP,
				"snapshot_tag", w.SnapshotTag,
				"keyspace", d.Keyspace,
				"table", d.Table,
				"dir", d.Path,
			)

			mp := path.Join(d.Path, backupspec.ScyllaManifest)
			if err := w.Client.RcloneDeleteFile(ctx, h.IP, mp); err != nil {
				if scyllaclient.StatusCodeOf(err) != http.StatusNotFound {
					w.Logger.Error(ctx, "Failed to delete local backupspec.file", "error", err)
				}
			}
			sp := path.Join(d.Path, backupspec.ScyllaSchema)
			if err := w.Client.RcloneDeleteFile(ctx, h.IP, sp); err != nil {
				if scyllaclient.StatusCodeOf(err) != http.StatusNotFound {
					w.Logger.Error(ctx, "Failed to delete local schema file", "error", err)
				}
			}

			var (
				files []fileInfo
				size  int64
			)
			opts := &scyllaclient.RcloneListDirOpts{
				FilesOnly:   true,
				ShowModTime: true,
			}
			err := w.Client.RcloneListDirIter(ctx, h.IP, d.Path, opts, func(f *scyllaclient.RcloneListDirItem) {
				// Filter out Scylla manifest and Schema files, they are not needed.
				if f.Name == backupspec.ScyllaManifest || f.Name == backupspec.ScyllaSchema {
					return
				}
				files = append(files, fileInfo{
					Name: f.Name,
					Size: f.Size,
				})
				size += f.Size
				if time.Time(f.ModTime).After(nftt) {
					d.NewFilesSize += size
				}
			})
			if err != nil {
				if scyllaclient.StatusCodeOf(err) == http.StatusNotFound {
					continue
				}
				return nil, errors.Wrap(err, "list table")
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
			d.SkippedBytesOffset = d.Progress.Skipped
			w.onRunProgress(ctx, d.Progress)

			dirs = append(dirs, d)
		}
	}

	// In case of reindexing, it's possible that all snapshot dirs
	// were already uploaded and deleted in the previous run (#3733).
	if w.PrevStage != StageUpload && len(dirs) == 0 {
		return nil, errors.New("could not find any files")
	}

	// Sort dirs in descending order by size of new files. This gives
	// the priority to the most active tables. The probability of compaction is
	// greater in tables that get more writes or were recently compacted.
	sort.Slice(dirs, func(i, j int) bool {
		return dirs[i].NewFilesSize > dirs[j].NewFilesSize
	})

	w.Logger.Debug(ctx, "Found snapshot directories", "host", h.IP, "count", len(dirs))
	return dirs, nil
}

func (w *worker) newFilesTimeThreshold() time.Time {
	t, err := backupspec.SnapshotTagTime(w.SnapshotTag)
	if err != nil {
		return time.Time{}
	}
	return t.Add(-24 * time.Hour)
}
