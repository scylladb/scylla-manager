// Copyright (C) 2017 ScyllaDB

package backup

import (
	"context"
	"path"
	"sort"
	"sync"
	"time"

	"github.com/scylladb/scylla-manager/v3/pkg/scyllaclient"
	"github.com/scylladb/scylla-manager/v3/pkg/util/backupmanifest"
	"github.com/scylladb/scylla-manager/v3/pkg/util/uuid"
)

// listManifestsInAllLocations returns manifests for all nodes of a in all
// locations specified in hosts.
func listManifestsInAllLocations(ctx context.Context, client *scyllaclient.Client, hosts []hostInfo, clusterID uuid.UUID) ([]*backupmanifest.ManifestInfo, error) {
	var (
		locations = make(map[backupmanifest.Location]struct{})
		manifests []*backupmanifest.ManifestInfo
	)

	for i := range hosts {
		if _, ok := locations[hosts[i].Location]; ok {
			continue
		}
		locations[hosts[i].Location] = struct{}{}

		lm, err := listManifests(ctx, client, hosts[i].IP, hosts[i].Location, clusterID)
		if err != nil {
			return nil, err
		}
		manifests = append(manifests, lm...)
	}

	return manifests, nil
}

// listManifests returns manifests for all nodes of a given cluster in the location.
// Manifests are sorted deterministically by their ClusterID, TaskID, SnapshotTag and NodeID.
// If cluster is uuid.Nil then it returns manifests for all clusters it can find.
func listManifests(ctx context.Context, client *scyllaclient.Client, host string, location backupmanifest.Location, clusterID uuid.UUID) ([]*backupmanifest.ManifestInfo, error) {
	baseDir := backupmanifest.RemoteMetaClusterDCDir(clusterID)
	if clusterID == uuid.Nil {
		baseDir = path.Join("backup", string(backupmanifest.MetaDirKind))
	}

	opts := scyllaclient.RcloneListDirOpts{
		FilesOnly: true,
		Recurse:   true,
	}

	var manifests []*backupmanifest.ManifestInfo
	err := client.RcloneListDirIter(ctx, host, location.RemotePath(baseDir), &opts, func(f *scyllaclient.RcloneListDirItem) {
		p := path.Join(baseDir, f.Path)
		m := &backupmanifest.ManifestInfo{}
		if err := m.ParsePath(p); err != nil {
			return
		}
		m.Location = location
		manifests = append(manifests, m)
	})
	if err != nil {
		return nil, err
	}
	// Sort manifests by ClusterID, TaskID, SnapshotTag and NodeID
	sort.Slice(manifests, func(i, j int) bool {
		if manifests[i].ClusterID != manifests[j].ClusterID {
			return manifests[i].ClusterID.String() < manifests[j].ClusterID.String()
		}
		if manifests[i].TaskID != manifests[j].TaskID {
			return manifests[i].TaskID.String() < manifests[j].TaskID.String()
		}
		if manifests[i].SnapshotTag != manifests[j].SnapshotTag {
			return manifests[i].SnapshotTag < manifests[j].SnapshotTag
		}
		return manifests[i].NodeID < manifests[j].NodeID
	})

	return manifests, nil
}

// ListFilter specifies manifest listing criteria.
type ListFilter struct {
	ClusterID   uuid.UUID `json:"cluster_id"`
	DC          string    `json:"dc"`
	NodeID      string    `json:"node_id"`
	TaskID      uuid.UUID `json:"task_id"`
	Keyspace    []string  `json:"keyspace"`
	SnapshotTag string    `json:"snapshot_tag"`
	MinDate     time.Time `json:"min_date"`
	MaxDate     time.Time `json:"max_date"`
	Temporary   bool      `json:"temporary"`
}

func (f *ListFilter) prune(m *backupmanifest.ManifestInfo) bool {
	filters := []func(m *backupmanifest.ManifestInfo) bool{
		f.pruneClusterID,
		f.pruneDC,
		f.pruneNodeID,
		f.pruneTaskID,
		f.pruneSnapshotTag,
		f.pruneTemporary,
	}
	for _, f := range filters {
		if f(m) {
			return true
		}
	}
	return false
}

func (f *ListFilter) pruneClusterID(m *backupmanifest.ManifestInfo) bool {
	if m.ClusterID != uuid.Nil && f.ClusterID != uuid.Nil {
		if m.ClusterID != f.ClusterID {
			return true
		}
	}
	return false
}

func (f *ListFilter) pruneDC(m *backupmanifest.ManifestInfo) bool {
	if m.DC != "" && f.DC != "" {
		if m.DC != f.DC {
			return true
		}
	}
	return false
}

func (f *ListFilter) pruneNodeID(m *backupmanifest.ManifestInfo) bool {
	if m.NodeID != "" && f.NodeID != "" {
		if m.NodeID != f.NodeID {
			return true
		}
	}
	return false
}

func (f *ListFilter) pruneTaskID(m *backupmanifest.ManifestInfo) bool {
	if m.TaskID != uuid.Nil && f.TaskID != uuid.Nil {
		if m.TaskID != f.TaskID {
			return true
		}
	}
	return false
}

func (f *ListFilter) pruneSnapshotTag(m *backupmanifest.ManifestInfo) bool {
	if m.SnapshotTag != "" {
		if f.SnapshotTag != "" {
			return m.SnapshotTag != f.SnapshotTag
		}
		if !f.MinDate.IsZero() && m.SnapshotTag < backupmanifest.SnapshotTagAt(f.MinDate) {
			return true
		}
		if !f.MaxDate.IsZero() && m.SnapshotTag > backupmanifest.SnapshotTagAt(f.MaxDate) {
			return true
		}
	}
	return false
}

func (f *ListFilter) pruneTemporary(m *backupmanifest.ManifestInfo) bool {
	if m.Temporary {
		return !f.Temporary
	}
	return false
}

func filterManifests(manifests []*backupmanifest.ManifestInfo, filter ListFilter) []*backupmanifest.ManifestInfo {
	var out []*backupmanifest.ManifestInfo
	for _, m := range manifests {
		if !filter.prune(m) {
			out = append(out, m)
		}
	}
	return out
}

func groupManifestsByNode(manifests []*backupmanifest.ManifestInfo) map[string][]*backupmanifest.ManifestInfo {
	v := map[string][]*backupmanifest.ManifestInfo{}
	for _, m := range manifests {
		v[m.NodeID] = append(v[m.NodeID], m)
	}
	return v
}

func groupManifestsByTask(manifests []*backupmanifest.ManifestInfo) map[uuid.UUID][]*backupmanifest.ManifestInfo {
	v := map[uuid.UUID][]*backupmanifest.ManifestInfo{}
	for _, m := range manifests {
		v[m.TaskID] = append(v[m.TaskID], m)
	}
	return v
}

// popNodeIDManifestsForLocation returns a function that for a given location
// finds next node and it's manifests from that location.
func popNodeIDManifestsForLocation(manifests []*backupmanifest.ManifestInfo) func(h hostInfo) (string, []*backupmanifest.ManifestInfo) {
	var mu sync.Mutex
	nodeIDManifests := groupManifestsByNode(manifests)
	return func(h hostInfo) (string, []*backupmanifest.ManifestInfo) {
		mu.Lock()
		defer mu.Unlock()

		// Fast path, get manifests for the current node
		if manifests, ok := nodeIDManifests[h.ID]; ok {
			delete(nodeIDManifests, h.ID)
			return h.ID, manifests
		}

		// Look for other nodes in the same location
		for nodeID, manifests := range nodeIDManifests {
			if manifests[0].Location == h.Location {
				delete(nodeIDManifests, nodeID)
				return nodeID, manifests
			}
		}

		return "", nil
	}
}
