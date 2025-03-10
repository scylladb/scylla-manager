// Copyright (C) 2023 ScyllaDB

package restore

import (
	"context"
	"path"
	"sort"

	"github.com/scylladb/scylla-manager/backupspec"
	"github.com/scylladb/scylla-manager/v3/pkg/scyllaclient"
	"go.uber.org/multierr"
)

func (w *worker) forEachManifest(ctx context.Context, location LocationInfo, f func(backupspec.ManifestInfoWithContent) error) error {
	// Load manifest content
	load := func(c *backupspec.ManifestContentWithIndex, m *backupspec.ManifestInfo) error {
		r, err := w.client.RcloneOpen(ctx, location.AnyHost(), m.Location.RemotePath(m.Path()))
		if err != nil {
			return err
		}
		return multierr.Append(c.Read(r), r.Close())
	}

	for _, m := range location.Manifest {
		c := new(backupspec.ManifestContentWithIndex)
		if err := load(c, m); err != nil {
			return err
		}

		err := f(backupspec.ManifestInfoWithContent{
			ManifestInfo:             m,
			ManifestContentWithIndex: c,
		})
		if err != nil {
			return err
		}
	}
	return nil
}

// getManifestInfo returns manifests with receiver's snapshot tag for all nodes in the location.
func (w *worker) getManifestInfo(ctx context.Context, host string, location backupspec.Location, snapshotTag string) ([]*backupspec.ManifestInfo, error) {
	baseDir := path.Join("backup", string(backupspec.MetaDirKind))
	opts := scyllaclient.RcloneListDirOpts{
		FilesOnly: true,
		Recurse:   true,
	}

	var manifests []*backupspec.ManifestInfo
	err := w.client.RcloneListDirIter(ctx, host, location.RemotePath(baseDir), &opts, func(f *scyllaclient.RcloneListDirItem) {
		m := new(backupspec.ManifestInfo)
		if err := m.ParsePath(path.Join(baseDir, f.Path)); err != nil {
			return
		}
		m.Location = location
		if m.SnapshotTag == snapshotTag {
			manifests = append(manifests, m)
		}
	})
	if err != nil {
		return nil, err
	}

	// Ensure deterministic order
	sort.Slice(manifests, func(i, j int) bool {
		return manifests[i].NodeID < manifests[j].NodeID
	})
	return manifests, nil
}
