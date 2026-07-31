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

	regular := map[backupspec.ManifestInfo]struct{}{}
	tmp := map[backupspec.ManifestInfo]struct{}{}
	err := w.client.RcloneListDirIter(ctx, host, location.RemotePath(metaBaseDir), &opts, func(f *scyllaclient.RcloneListDirItem) {
		m := new(backupspec.ManifestInfo)
		if err := m.ParsePath(path.Join(metaBaseDir, f.Path)); err != nil {
			return
		}
		m.Location = location
		if m.ClusterID != clusterID || m.SnapshotTag != snapshotTag {
			return
		}
		if m.Temporary {
			tmp[*m] = struct{}{}
			return
		}
		regular[*m] = struct{}{}
	})
	if err != nil {
		return nil, err
	}
	// Validate that the only encountered temporary manifests
	// are shadowed by regular ones - otherwise we are trying
	// to restore partial backup.
	for m := range tmp {
		r := m
		r.Temporary = false
		if _, ok := regular[r]; !ok {
			return nil, errors.Errorf("temporary manifest %s is not shadowed by regular manifest. "+
				"This might mean that snapshot %s wasn't fully uploaded or that it was partially deleted. "+
				"Validate snapshot correctness and remove/promote the temporary manifest before proceeding.", m.Path(), snapshotTag)
		}
	}
	// Don't return shadowed temporary manifests.
	manifests := make([]*backupspec.ManifestInfo, 0, len(regular))
	for m := range regular {
		manifests = append(manifests, new(m))
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
