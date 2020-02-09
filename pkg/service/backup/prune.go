// Copyright (C) 2017 ScyllaDB

package backup

import (
	"github.com/scylladb/scylla-manager/pkg/backup"
	"github.com/scylladb/scylla-manager/pkg/util/uuid"
)

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

func pruneTaskID(taskID uuid.UUID, filter ListFilter) bool {
	if taskID != uuid.Nil && filter.TaskID != uuid.Nil {
		if taskID != filter.TaskID {
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
		if !f.MinDate.IsZero() && snapshotTag < backup.SnapshotTagAt(f.MinDate) {
			return true
		}
		if !f.MaxDate.IsZero() && snapshotTag > backup.SnapshotTagAt(f.MaxDate) {
			return true
		}
	}
	return false
}

func makeListFilterPruneDirFunc(f ListFilter) func(string) bool {
	return func(dir string) bool {
		var m backup.RemoteManifest

		// Discard invalid paths
		if err := m.ParsePartialPath(dir); err != nil {
			return true
		}
		if pruneClusterID(m.ClusterID, f) || pruneSnapshotTag(m.SnapshotTag, f) ||
			pruneDC(m.DC, f) || pruneNodeID(m.NodeID, f) || pruneTaskID(m.TaskID, f) {
			return true
		}

		return false
	}
}
