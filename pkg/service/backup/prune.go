// Copyright (C) 2017 ScyllaDB

package backup

import "github.com/scylladb/mermaid/pkg/util/uuid"

func pruneDC(dc string, filter ListFilter) bool {
	if dc != "" && filter.DC != "" {
		if dc != filter.DC {
			return true
		}
	}
	return false
}

func pruneNodeID(nodeID string, filter ListFilter) bool {
	if nodeID != "" && filter.NodeID != "" {
		if nodeID != filter.NodeID {
			return true
		}
	}
	return false
}

func pruneClusterID(clusterID uuid.UUID, filter ListFilter) bool {
	if clusterID != uuid.Nil && filter.ClusterID != uuid.Nil {
		if clusterID != filter.ClusterID {
			return true
		}
	}
	return false
}

func pruneSnapshotTag(snapshotTag string, f ListFilter) bool {
	// Filter snapshot tags
	if snapshotTag != "" {
		if f.SnapshotTag != "" {
			return snapshotTag != f.SnapshotTag
		}
		if !f.MinDate.IsZero() && snapshotTag < snapshotTagAt(f.MinDate) {
			return true
		}
		if !f.MaxDate.IsZero() && snapshotTag > snapshotTagAt(f.MaxDate) {
			return true
		}
	}
	return false
}

func makeListFilterPruneDirFunc(f ListFilter) func(string) bool {
	return func(dir string) bool {
		var m remoteManifest

		// Discard invalid paths
		if err := m.ParsePartialPath(dir); err != nil {
			return true
		}
		if pruneClusterID(m.ClusterID, f) || pruneSnapshotTag(m.SnapshotTag, f) ||
			pruneDC(m.DC, f) || pruneNodeID(m.NodeID, f) {
			return true
		}

		return false
	}
}
