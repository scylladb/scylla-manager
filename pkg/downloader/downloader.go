// Copyright (C) 2017 ScyllaDB

package downloader

import (
	"context"
	"path"
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

	fsrc fs.Fs
	fdst fs.Fs
}

func New(l backup.Location, dataDir string, logger log.Logger) (*Downloader, error) {
	// Temporary context to satisfy rclone
	ctx := context.Background()

	// Init file systems, we want to reuse the rclone Fs instances as they
	// hold memory buffers.
	fsrc, err := fs.NewFs(ctx, l.RemotePath(""))
	if err != nil {
		return nil, errors.Wrap(err, "init location")
	}
	fdst, err := fs.NewFs(ctx, dataDir)
	if err != nil {
		return nil, errors.Wrap(err, "init data dir")
	}

	return &Downloader{
		logger: logger,
		fsrc:   fsrc,
		fdst:   fdst,
	}, nil
}

// WithKeyspace sets the keyspace/table filters.
func (d *Downloader) WithKeyspace(filters []string) (*Downloader, error) {
	ksf, err := ksfilter.NewFilter(filters)
	if err != nil {
		return d, errors.Wrap(err, "keyspace/table filter")
	}
	d.keyspace = ksf

	return d, nil
}

// WithClearTables would delete any data forom a table before downloading new
// files. It does not work with SSTableLoaderTableDirMode mode.
func (d *Downloader) WithClearTables() *Downloader {
	d.clearTables = true
	return d
}

// WithTableDirMode specifies type of resulting directory structure.
func (d *Downloader) WithTableDirMode(mode TableDirMode) *Downloader {
	d.mode = mode
	return d
}

// WithDryRun turns on the dry-run mode where no data operations are performed.
func (d *Downloader) WithDryRun() *Downloader {
	d.dryRun = true
	return d
}

// Download executes download operation by taking snapshot files from configured
// locations and downloading them to the data directory.
func (d *Downloader) Download(ctx context.Context, m *backup.RemoteManifest) error {
	d.logger.Info(ctx, "Initializing downloader",
		"cluster_id", m.ClusterID,
		"cluster_name", m.Content.ClusterName,
		"node_id", m.NodeID,
		"node_ip", m.Content.IP,
		"filter", d.keyspace.Filters(),
		"mode", d.mode,
		"clear_tables", d.clearTables,
		"dry-run", d.dryRun,
	)

	index := d.filteredIndex(ctx, m)

	// Spawn all downloads at the same time, we rely on rclone ability to limit
	// nr. of transfers.
	return parallel.Run(len(index), parallel.NoLimit, func(i int) error {
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

	if d.mode == SSTableLoaderTableDirMode {
		d.logger.Info(ctx, "Clear tables is not supported with flat table dir")
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
	var tableDirs []fs.Directory
	r := regexp.MustCompile("^" + u.Table + "-([a-f0-9]{32})$")
	entries.ForDir(func(dir fs.Directory) {
		if r.MatchString(path.Base(dir.String())) {
			tableDirs = append(tableDirs, dir)
		}
	})

	// Delete all files in table dir at depth 1
	for _, dir := range tableDirs {
		d.logger.Info(ctx, "Clearing table dir", "path", dir.String())
		if d.dryRun {
			continue
		}

		entries, err := d.fdst.List(ctx, dir.String())
		if err != nil {
			return errors.Wrapf(err, "list %s", dir.String())
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
