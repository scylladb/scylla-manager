// Copyright (C) 2025 ScyllaDB

package one2onerestore

import (
	"context"
	"path"

	"github.com/pkg/errors"
	"github.com/scylladb/scylla-manager/backupspec"
	"github.com/scylladb/scylla-manager/v3/pkg/util/uuid"

	"github.com/scylladb/scylla-manager/v3/pkg/scyllaclient"
)

// getManifestInfo returns manifests with receiver's snapshot tag for all nodes in the location.
func (w *worker) getManifestInfo(ctx context.Context, host, snapshotTag string, clusterID uuid.UUID, location backupspec.Location) ([]*backupspec.ManifestInfo, error) {
	metaBaseDir := path.Join("backup", string(backupspec.MetaDirKind))

	opts := scyllaclient.RcloneListDirOpts{
		FilesOnly: true,
		Recurse:   true,
	}

	var manifests []*backupspec.ManifestInfo
	err := w.client.RcloneListDirIter(ctx, host, location.RemotePath(metaBaseDir), &opts, func(f *scyllaclient.RcloneListDirItem) {
		m := new(backupspec.ManifestInfo)
		if err := m.ParsePath(path.Join(metaBaseDir, f.Path)); err != nil {
			return
		}
		m.Location = location
		if m.ClusterID == clusterID && m.SnapshotTag == snapshotTag {
			manifests = append(manifests, m)
		}
	})
	if err != nil {
		return nil, err
	}
	return manifests, nil
}

func (w *worker) getManifestContent(ctx context.Context, host string, manifest *backupspec.ManifestInfo) (*backupspec.ManifestContentWithIndex, error) {
	mc := &backupspec.ManifestContentWithIndex{}
	r, err := w.client.RcloneOpen(ctx, host, manifest.Location.RemotePath(manifest.Path()))
	if err != nil {
		return nil, errors.Wrap(err, "open manifest")
	}
	defer r.Close()
	if err := mc.Read(r); err != nil {
		return nil, errors.Wrap(err, "read manifest")
	}
	return mc, nil
}
