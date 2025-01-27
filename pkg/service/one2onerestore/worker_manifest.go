// Copyright (C) 2025 ScyllaDB

package one2onerestore

import (
	"context"
	"path"
	"slices"
	"strings"

	. "github.com/scylladb/scylla-manager/v3/pkg/service/backup/backupspec"

	"github.com/scylladb/scylla-manager/v3/pkg/scyllaclient"
)

// getManifestInfo returns manifests with receiver's snapshot tag for all nodes in the location.
func (w *worker) getManifestInfo(ctx context.Context, host, snapshotTag string, location Location) ([]*ManifestInfo, error) {
	baseDir := path.Join("backup", string(MetaDirKind))
	opts := scyllaclient.RcloneListDirOpts{
		FilesOnly: true,
		Recurse:   true,
	}

	var manifests []*ManifestInfo
	err := w.client.RcloneListDirIter(ctx, host, location.RemotePath(baseDir), &opts, func(f *scyllaclient.RcloneListDirItem) {
		m := new(ManifestInfo)
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
	slices.SortFunc(manifests, func(a, b *ManifestInfo) int {
		return strings.Compare(a.NodeID, b.NodeID)
	})
	return manifests, nil
}
