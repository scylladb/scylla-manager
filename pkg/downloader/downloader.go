// Copyright (C) 2017 ScyllaDB

package downloader

import (
	"context"
	"os"
	"os/user"
	"path"
	"path/filepath"
	"regexp"

	"github.com/pkg/errors"
	"github.com/rclone/rclone/fs"
	"github.com/rclone/rclone/fs/operations"
	"github.com/rclone/rclone/fs/sync"
	"github.com/scylladb/go-log"
	backup "github.com/scylladb/scylla-manager/pkg/service/backup/backupspec"
	"github.com/scylladb/scylla-manager/pkg/util/inexlist/ksfilter"
	"github.com/scylladb/scylla-manager/pkg/util/parallel"
)

// Downloader reads manifest for the provided snapshot tag, cluster and node.
// It then downloads files for tables matching the filter to the provided data dir.
// It also supports downloading to upload directory and downloading in a format
// suitable for sstable loader see TableDirMode.
type Downloader struct {
	logger      log.Logger
	keyspace    *ksfilter.Filter
	mode        TableDirMode
	clearTables bool
	dryRun      bool
	plan        Plan

	fsrc fs.Fs
	fdst fs.Fs
}

func New(l backup.Location, dataDir string, logger log.Logger, opts ...Option) (*Downloader, error) {
	// Temporary context to satisfy rclone.
	ctx := context.Background()

	absDataDir, err := filepath.Abs(dataDir)
	if err != nil {
		return nil, errors.Wrap(err, "get data directory absolute path")
	}

	// Check if the current user is the data directory owner.
	o, err := dirOwner(absDataDir)
	if os.IsNotExist(err) {
		return nil, err
	}
	if err != nil {
		return nil, errors.Wrap(err, absDataDir)
	}
	u, err := user.Current()
	if err != nil {
		return nil, errors.Wrap(err, "get current user")
	}
	if o.Uid != u.Uid {
		return nil, errors.Errorf("run command as %s (UID:%s)", o.Name, o.Uid)
	}

	// Init file systems, we want to reuse the rclone Fs instances as they
	// hold memory buffers.
	fsrc, err := fs.NewFs(ctx, l.RemotePath(""))
	if err != nil {
		return nil, errors.Wrap(err, "init location")
	}
	fdst, err := fs.NewFs(ctx, absDataDir)
	if err != nil {
		return nil, errors.Wrap(err, "init data dir")
	}

	d := &Downloader{
		logger: logger,
		fsrc:   fsrc,
		fdst:   fdst,
	}
	for _, o := range opts {
		if err := o(d); err != nil {
			return nil, err
		}
	}
	return d, nil
}

// DryRun returns an action plan without performing any disk operations.
func (d *Downloader) DryRun(ctx context.Context, m *backup.RemoteManifest) (Plan, error) {
	d.dryRun = true
	d.plan = Plan{m: m}
	return d.plan, d.download(ctx, m, 1)
}

// Download executes download operation by taking snapshot files from configured
// locations and downloading them to the data directory.
func (d *Downloader) Download(ctx context.Context, m *backup.RemoteManifest) error {
	d.dryRun = false
	return d.download(ctx, m, parallel.NoLimit)
}

func (d *Downloader) download(ctx context.Context, m *backup.RemoteManifest, workers int) error {
	d.logger.Info(ctx, "Initializing downloader",
		"cluster_id", m.ClusterID,
		"cluster_name", m.Content.ClusterName,
		"node_id", m.NodeID,
		"node_ip", m.Content.IP,
		"filter", d.keyspace.Filters(),
		"mode", d.mode,
		"clear_tables", d.clearTables,
	)

	index := d.filteredIndex(ctx, m)

	// Check if we have enough disk space.
	var size int64
	for _, u := range index {
		size += u.Size
	}
	usage, err := d.fdst.(fs.Abouter).About(ctx)
	if err != nil {
		return errors.Wrap(err, "check disk size")
	}
	if usage.Free == nil {
		d.logger.Info(ctx, "Failed to get free bytes", "usage", usage)
	} else if *usage.Free < size {
		return errors.Errorf("not enought disk space free %s required %s", fs.SizeSuffix(*usage.Free), fs.SizeSuffix(size))
	}

	// Spawn all downloads at the same time, we rely on rclone ability to limit
	// nr. of transfers.
	return parallel.Run(len(index), workers, func(i int) error {
		u := index[i]

		if err := d.clearTableIfNeeded(ctx, u); err != nil {
			return errors.Wrapf(err, "clear table %s.%s", u.Keyspace, u.Table)
		}

		if len(u.Files) == 0 {
			d.logger.Info(ctx, "Skipping empty", "keyspace", u.Keyspace, "table", u.Table)
			return nil
		}

		if err := d.downloadFiles(ctx, m, u); err != nil {
			return errors.Wrapf(err, "download table %s.%s", u.Keyspace, u.Table)
		}

		return nil
	})
}

func (d *Downloader) filteredIndex(ctx context.Context, m *backup.RemoteManifest) []backup.FilesMeta {
	if d.keyspace == nil {
		return m.Content.Index
	}

	var index []backup.FilesMeta
	for _, u := range m.Content.Index {
		if !d.shouldDownload(u.Keyspace, u.Table) {
			d.logger.Debug(ctx, "Table filtered out", "keyspace", u.Keyspace, "table", u.Table)
		} else {
			index = append(index, u)
		}
	}
	return index
}

func (d *Downloader) shouldDownload(keyspace, table string) bool {
	return d.keyspace == nil || d.keyspace.Check(keyspace, table)
}

func (d *Downloader) clearTableIfNeeded(ctx context.Context, u backup.FilesMeta) error {
	if !d.clearTables {
		return nil
	}

	// List all tables and versions
	entries, err := d.fdst.List(ctx, u.Keyspace)
	if errors.Is(err, fs.ErrorDirNotFound) {
		return nil
	}
	if err != nil {
		return errors.Wrap(err, "list tables")
	}

	// Find all versioned table dirs
	var tableDirs []string

	switch d.mode {
	case DefaultTableDirMode:
		r := regexp.MustCompile("^" + u.Table + "-([a-f0-9]{32})$")
		entries.ForDir(func(dir fs.Directory) {
			if r.MatchString(path.Base(dir.String())) {
				tableDirs = append(tableDirs, dir.String())
			}
		})
	case UploadTableDirMode, SSTableLoaderTableDirMode:
		tableDirs = append(tableDirs, d.dstDir(u))
	}

	// Delete all files in table dir at depth 1
	for _, dir := range tableDirs {
		entries, err := d.fdst.List(ctx, dir)
		if errors.Is(err, fs.ErrorDirNotFound) {
			continue
		}
		if err != nil {
			return errors.Wrapf(err, "list %s", dir)
		}

		d.logger.Info(ctx, "Clearing table dir", "path", dir)

		if d.dryRun {
			d.plan.ClearActions = append(d.plan.ClearActions, ClearAction{
				Keyspace: u.Keyspace,
				Table:    u.Table,
				Dir:      dir,
			})
			continue
		}

		if err := entries.ForObjectError(func(o fs.Object) error {
			return errors.Wrapf(operations.DeleteFile(ctx, o), "delete file %s", o)
		}); err != nil {
			return err
		}
	}

	return nil
}

func (d *Downloader) downloadFiles(ctx context.Context, m *backup.RemoteManifest, u backup.FilesMeta) error {
	d.logger.Info(ctx, "Downloading",
		"keyspace", u.Keyspace,
		"table", u.Table,
		"files", len(u.Files),
		"size", u.Size,
	)

	if d.dryRun {
		d.plan.DownloadActions = append(d.plan.DownloadActions, DownloadAction{
			Keyspace: u.Keyspace,
			Table:    u.Table,
			Size:     u.Size,
			Dir:      d.dstDir(u),
		})
		return nil
	}

	return sync.CopyPaths(ctx, d.fdst, d.dstDir(u), d.fsrc, m.RemoteSSTableVersionDir(u.Keyspace, u.Table, u.Version), u.Files, false)
}

func (d *Downloader) dstDir(u backup.FilesMeta) (dir string) {
	switch d.mode {
	case DefaultTableDirMode:
		dir = path.Join(u.Keyspace, u.Table+"-"+u.Version)
	case UploadTableDirMode:
		dir = path.Join(u.Keyspace, u.Table+"-"+u.Version, "upload")
	case SSTableLoaderTableDirMode:
		dir = path.Join(u.Keyspace, u.Table)
	}
	return
}

// Root returns the root destination directory.
func (d *Downloader) Root() string {
	return d.fdst.Root()
}
