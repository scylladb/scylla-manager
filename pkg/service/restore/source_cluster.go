// Copyright (C) 2026 ScyllaDB

package restore

import (
	"github.com/scylladb/scylla-manager/backupspec"
	"github.com/scylladb/scylla-manager/v3/pkg/util/uuid"
)

// filterManifestsBySourceCluster keeps only the manifests of the source
// cluster. When sourceClusterID is unset, all manifests are kept.
func filterManifestsBySourceCluster(manifests []*backupspec.ManifestInfo, sourceClusterID uuid.UUID) []*backupspec.ManifestInfo {
	if sourceClusterID == uuid.Nil {
		return manifests
	}
	var out []*backupspec.ManifestInfo
	for _, m := range manifests {
		if m.ClusterID == sourceClusterID {
			out = append(out, m)
		}
	}
	return out
}
