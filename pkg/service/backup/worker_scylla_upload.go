// Copyright (C) 2024 ScyllaDB

package backup

import (
	"slices"

	. "github.com/scylladb/scylla-manager/v3/pkg/service/backup/backupspec"
)

// Decides whether we should use Scylla backup API for uploading the files.
func (w *worker) useScyllaBackupAPI(ctx context.Context, d snapshotDir, hi hostInfo) (bool, error) {
	// Scylla backup API does not handle creation of versioned files.
	if d.willCreateVersioned {
		return false, nil
	}
	// List of object storage providers supported by Scylla backup API.
	scyllaSupportedProviders := []Provider{
		S3,
	}
	if !slices.Contains(scyllaSupportedProviders, hi.Location.Provider) {
		return false, nil
	}
	nc, err := w.nodeInfo(ctx, hi.IP)
	if err != nil {
		return false, errors.Wrapf(err, "get node %s info", hi.IP)
	}
	return nc.SupportsScyllaBackupRestoreAPI()
}
