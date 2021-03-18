// Copyright (C) 2017 ScyllaDB

package downloader

import (
	"context"
	"path"

	"github.com/pkg/errors"
	"github.com/rclone/rclone/fs"
	"github.com/rclone/rclone/fs/walk"
	"github.com/scylladb/go-set/strset"
	backup "github.com/scylladb/scylla-manager/pkg/service/backup/backupspec"
	"github.com/scylladb/scylla-manager/pkg/util/uuid"
)

// ManifestLookupCriteria specifies which manifest you want to use to download
// files.
type ManifestLookupCriteria struct {
	NodeID      uuid.UUID
	SnapshotTag string
}

func (c ManifestLookupCriteria) matches(m *backup.RemoteManifest) bool {
	return c.NodeID.String() == m.NodeID && c.SnapshotTag == m.SnapshotTag
}

// LookupManifest finds and loads manifest base on the lookup criteria.
func (d *Downloader) LookupManifest(ctx context.Context, c ManifestLookupCriteria) (*backup.RemoteManifest, error) {
	d.logger.Info(ctx, "Searching for manifest", "criteria", c)

	baseDir := path.Join("backup", string(backup.MetaDirKind))

	var (
		manifest  *backup.RemoteManifest
		snapshots = strset.New()
		nodes     = strset.New()
	)

	lookup := func(entries fs.DirEntries) error {
		if manifest != nil {
			return nil
		}

		var m backup.RemoteManifest
		for _, entry := range entries {
			d.logger.Debug(ctx, "Found manifest dir entry", "path", entry.String())

			o, ok := entry.(fs.Object)
			if !ok {
				continue
			}
			if err := m.ParsePartialPath(o.String()); err != nil {
				continue
			}
			if m.SnapshotTag == "" || m.Temporary {
				continue
			}
			if c.matches(&m) {
				if err := readManifestContentFromObject(ctx, &m, o); err != nil {
					return errors.Wrap(err, "read content")
				}

				// Note: returning error here would stop the walk but it would
				// also log rclone ERROR. We want to avoid that as it can be
				// misleading for CLI users.
				manifest = &m
				return nil
			}

			snapshots.Add(m.SnapshotTag)
			nodes.Add(m.NodeID)
		}

		return nil
	}

	err := walk.ListR(ctx, d.fsrc, baseDir, true, backup.RemoteManifestLevel(baseDir)+1, walk.ListObjects, lookup)
	if manifest != nil {
		d.logger.Info(ctx, "Found manifest", "path", manifest.RemoteManifestFile())
		return manifest, err
	}
	if err != nil {
		return nil, err
	}

	if !nodes.Has(c.NodeID.String()) {
		return nil, errors.Errorf("unknown node ID %q", c.NodeID)
	}

	return nil, errors.Errorf("unknown snapshot tag %q, available snapshots: %s", c.SnapshotTag, snapshots)
}

func readManifestContentFromObject(ctx context.Context, m *backup.RemoteManifest, o fs.Object) error {
	r, err := o.Open(ctx)
	if err != nil {
		return err
	}
	defer r.Close()
	return m.ReadContent(r)
}
