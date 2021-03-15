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
	"github.com/scylladb/scylla-manager/pkg/util/uuid"
)

// TableDir specifies type of desired download.
type TableDir byte

const (
	// DefaultTableDir is intended for normal on node download when Scylla
	// server is shutdown. It downloads files directly to versioned table
	// directory.
	DefaultTableDir TableDir = iota
	// UploadTableDir uses upload dir inside versioned table directory.
	// It should be used for downloads when Scylla server is running.
	UploadTableDir
	// FlatTableDir is intended for use with sstable loader where the resulting
	// data structure it keyspace/table/files.
	FlatTableDir
)

// Downloader reads manifest for the provided snapshot tag, cluster and node.
// It then downloads files for tables matching the filter to the provided data dir.
// It also supports downloading to upload directory and downloading in a format
// suitable for sstable loader see TableDir.
type Downloader struct {
	clusterID   uuid.UUID
	dc          string
	nodeID      uuid.UUID
	snapshotTag string
	logger      log.Logger

	keyspace    *ksfilter.Filter
	mode        TableDir
	clearTables bool
	dryRun      bool

	fsrc fs.Fs
	fdst fs.Fs
}

func New(location backup.Location, clusterID uuid.UUID, dc string, nodeID uuid.UUID, snapshotTag, dataDir string, logger log.Logger) (*Downloader, error) {
	// Temporary context to satisfy rclone
	ctx := context.Background()

	// Init file systems, we want to reuse the rclone Fs instances as they
	// hold memory buffers.
	fsrc, err := fs.NewFs(ctx, location.RemotePath(""))
	if err != nil {
		return nil, errors.Wrap(err, "init location")
	}
	fdst, err := fs.NewFs(ctx, dataDir)
	if err != nil {
		return nil, errors.Wrap(err, "init data dir")
	}

	return &Downloader{
		clusterID:   clusterID,
		dc:          dc,
		nodeID:      nodeID,
		snapshotTag: snapshotTag,
		logger:      logger,

		fsrc: fsrc,
		fdst: fdst,
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
// files. It does not work with FlatTableDir mode.
func (d *Downloader) WithClearTables() *Downloader {
	d.clearTables = true
	return d
}

// WithMode specifies type of resulting directory structure.
func (d *Downloader) WithMode(mode TableDir) *Downloader {
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
func (d *Downloader) Download(ctx context.Context) error {
	m, err := d.manifest(ctx)
	if err != nil {
		return err
	}
	d.logger.Info(ctx, "Loaded manifest", "path", m.RemoteManifestFile())

	for _, u := range m.Content.Index {
		if !d.shouldDownload(u.Keyspace, u.Table) {
			d.logger.Debug(ctx, "Table filtered out", "keyspace", u.Keyspace, "table", u.Table)
			continue
		}

		if err := d.clearTableIfNeeded(ctx, u); err != nil {
			return errors.Wrapf(err, "clear table %s.%s", u.Keyspace, u.Table)
		}

		if len(u.Files) == 0 {
			d.logger.Info(ctx, "Empty", "keyspace", u.Keyspace, "table", u.Table)
			continue
		}

		if err := d.downloadFiles(ctx, m, u); err != nil {
			return errors.Wrapf(err, "download table %s.%s", u.Keyspace, u.Table)
		}
	}

	return nil
}

func (d *Downloader) manifest(ctx context.Context) (*backup.RemoteManifest, error) {
	dir := backup.RemoteManifestDir(d.clusterID, d.dc, d.nodeID.String())

	entries, err := d.fsrc.List(ctx, dir)
	if err != nil {
		return nil, err
	}
	if len(entries) == 0 {
		return nil, errors.New("no manifests found")
	}

	var (
		m         backup.RemoteManifest
		snapshots []string
	)
	for _, entry := range entries {
		d.logger.Debug(ctx, "Found manifest dir entry", "path", entry.String())

		o, ok := entry.(fs.Object)
		if !ok {
			continue
		}
		if err := m.ParsePartialPath(o.String()); err != nil {
			continue
		}
		if m.SnapshotTag == "" {
			continue
		}
		if m.SnapshotTag == d.snapshotTag {
			if m.Temporary {
				return nil, errors.New("temporary manifest, files may be missing")
			}
			if err := readManifestContentFromObject(ctx, &m, o); err != nil {
				return nil, errors.Wrap(err, "read content")
			}
			return &m, nil
		}

		snapshots = append(snapshots, m.SnapshotTag)
	}

	return nil, errors.Errorf("no manifest found, available snapshots are: %s", snapshots)
}

func readManifestContentFromObject(ctx context.Context, m *backup.RemoteManifest, o fs.Object) error {
	r, err := o.Open(ctx)
	if err != nil {
		return err
	}
	defer r.Close()
	return m.ReadContent(r)
}

func (d *Downloader) shouldDownload(keyspace, table string) bool {
	return d.keyspace == nil || d.keyspace.Check(keyspace, table)
}

func (d *Downloader) clearTableIfNeeded(ctx context.Context, u backup.FilesMeta) error {
	if !d.clearTables {
		return nil
	}

	if d.mode == FlatTableDir {
		d.logger.Info(ctx, "Clear tables is not supported with flat table dir")
		return nil
	}

	// List all tables and versions
	entries, err := d.fdst.List(ctx, u.Keyspace)
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
	d.logger.Info(ctx, "Downloading", "keyspace", u.Keyspace, "table", u.Table, "files", len(u.Files), "size", u.Size)

	if d.dryRun {
		return nil
	}

	return sync.CopyPaths(ctx, d.fdst, d.dstDir(u), d.fsrc, m.RemoteSSTableVersionDir(u.Keyspace, u.Table, u.Version), u.Files, false)
}

func (d *Downloader) dstDir(u backup.FilesMeta) (dir string) {
	switch d.mode {
	case DefaultTableDir:
		dir = path.Join(u.Keyspace, u.Table+"-"+u.Version)
	case UploadTableDir:
		dir = path.Join(u.Keyspace, u.Table+"-"+u.Version, "upload")
	case FlatTableDir:
		dir = path.Join(u.Keyspace, u.Table)
	}
	return
}

// Root returns the root destination directory.
func (d *Downloader) Root() string {
	return d.fdst.Root()
}
