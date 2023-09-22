// Copyright (C) 2023 ScyllaDB

package restore

import (
	"context"
	"fmt"
	"path"
	"sort"

	"github.com/pkg/errors"
	"github.com/scylladb/scylla-manager/v3/pkg/scyllaclient"
	. "github.com/scylladb/scylla-manager/v3/pkg/service/backup/backupspec"
	"github.com/scylladb/scylla-manager/v3/pkg/util/slice"
	"go.uber.org/multierr"
)

func (w *worker) forEachManifest(ctx context.Context, location Location, f func(ManifestInfoWithContent) error) error {
	closest := w.client.Config().Hosts
	hosts, ok := w.target.locationHosts[location]
	if !ok {
		return fmt.Errorf("no hosts for location %s", location)
	}

	var host string
	for _, h := range closest {
		if slice.ContainsString(hosts, h) {
			host = h
			break
		}
	}
	if host == "" {
		host = hosts[0]
	}

	manifests, err := w.getManifestInfo(ctx, host, location)
	if err != nil {
		return errors.Wrap(err, "list manifests")
	}

	// Load manifest content
	load := func(c *ManifestContentWithIndex, m *ManifestInfo) error {
		r, err := w.client.RcloneOpen(ctx, host, m.Location.RemotePath(m.Path()))
		if err != nil {
			return err
		}
		return multierr.Append(c.Read(r), r.Close())
	}

	for _, m := range manifests {
		c := new(ManifestContentWithIndex)
		if err := load(c, m); err != nil {
			return err
		}

		err := f(ManifestInfoWithContent{
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
func (w *worker) getManifestInfo(ctx context.Context, host string, location Location) ([]*ManifestInfo, error) {
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
		if m.SnapshotTag == w.run.SnapshotTag {
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
