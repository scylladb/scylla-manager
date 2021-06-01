// Copyright (C) 2017 ScyllaDB

package backup

import (
	"context"
	"path"
	"sync"
	"time"

	"github.com/scylladb/scylla-manager/pkg/scyllaclient"
	. "github.com/scylladb/scylla-manager/pkg/service/backup/backupspec"
	"github.com/scylladb/scylla-manager/pkg/util/uuid"
)

// listManifestsInAllLocations returns manifests for all nodes of a in all
// locations specified in hosts.
func listManifestsInAllLocations(ctx context.Context, client *scyllaclient.Client, hosts []hostInfo, clusterID uuid.UUID) ([]*RemoteManifest, error) {
	var (
		locations = make(map[Location]struct{})
		manifests []*RemoteManifest
	)

	for _, hi := range hosts {
		if _, ok := locations[hi.Location]; ok {
			continue
		}
		locations[hi.Location] = struct{}{}

		lm, err := listManifests(ctx, client, hi.IP, hi.Location, clusterID)
		if err != nil {
			return nil, err
		}
		manifests = append(manifests, lm...)
	}

	return manifests, nil
}

// listManifests returns manifests for all nodes of a given cluster in the location.
// If cluster is uuid.Nil then it returns manifests for all clusters it can find.
func listManifests(ctx context.Context, client *scyllaclient.Client, host string, location Location, clusterID uuid.UUID) ([]*RemoteManifest, error) {
	baseDir := RemoteMetaClusterDCDir(clusterID)
	if clusterID == uuid.Nil {
		baseDir = path.Join("backup", string(MetaDirKind))
	}

	opts := scyllaclient.RcloneListDirOpts{
		FilesOnly: true,
		Recurse:   true,
	}
	items, err := client.RcloneListDir(ctx, host, location.RemotePath(baseDir), &opts)
	if err != nil {
		return nil, err
	}

	var manifests []*RemoteManifest
	for _, item := range items {
		p := path.Join(baseDir, item.Path)
		m := &RemoteManifest{}
		if err := m.ParsePartialPath(p); err != nil {
			continue
		}
		m.Location = location
		manifests = append(manifests, m)
	}
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

func (f *ListFilter) prune(m *RemoteManifest) bool {
	filters := []func(m *RemoteManifest) bool{
		f.pruneDC,
		f.pruneNodeID,
		f.pruneClusterID,
		f.pruneTaskID,
		f.pruneSnapshotTag,
	}
	for _, f := range filters {
		if f(m) {
			return true
		}
	}
	return false
}

func (f *ListFilter) pruneDC(m *RemoteManifest) bool {
	if m.DC != "" && f.DC != "" {
		if m.DC != f.DC {
			return true
		}
	}
	return false
}

func (f *ListFilter) pruneNodeID(m *RemoteManifest) bool {
	if m.NodeID != "" && f.NodeID != "" {
		if m.NodeID != f.NodeID {
			return true
		}
	}
	return false
}

func (f *ListFilter) pruneClusterID(m *RemoteManifest) bool {
	if m.ClusterID != uuid.Nil && f.ClusterID != uuid.Nil {
		if m.ClusterID != f.ClusterID {
			return true
		}
	}
	return false
}

func (f *ListFilter) pruneTaskID(m *RemoteManifest) bool {
	if m.TaskID != uuid.Nil && f.TaskID != uuid.Nil {
		if m.TaskID != f.TaskID {
			return true
		}
	}
	return false
}

func (f *ListFilter) pruneSnapshotTag(m *RemoteManifest) bool {
	if m.SnapshotTag != "" {
		if f.SnapshotTag != "" {
			return m.SnapshotTag != f.SnapshotTag
		}
		if !f.MinDate.IsZero() && m.SnapshotTag < SnapshotTagAt(f.MinDate) {
			return true
		}
		if !f.MaxDate.IsZero() && m.SnapshotTag > SnapshotTagAt(f.MaxDate) {
			return true
		}
	}
	return false
}

func filterManifests(manifests []*RemoteManifest, filter ListFilter) []*RemoteManifest {
	var out []*RemoteManifest
	for _, m := range manifests {
		if !filter.prune(m) {
			out = append(out, m)
		}
	}
	return out
}

func groupManifestsByNode(manifests []*RemoteManifest) map[string][]*RemoteManifest {
	v := map[string][]*RemoteManifest{}
	for _, m := range manifests {
		v[m.NodeID] = append(v[m.NodeID], m)
	}
	return v
}

func groupManifestsByTask(manifests []*RemoteManifest) map[uuid.UUID][]*RemoteManifest {
	v := map[uuid.UUID][]*RemoteManifest{}
	for _, m := range manifests {
		v[m.TaskID] = append(v[m.TaskID], m)
	}
	return v
}

// popNodeIDManifestsForLocation returns a function that for a given location
// finds next node and it's manifests from that location.
func popNodeIDManifestsForLocation(manifests []*RemoteManifest) func(h hostInfo) (string, []*RemoteManifest) {
	var mu sync.Mutex
	nodeIDManifests := groupManifestsByNode(manifests)
	return func(h hostInfo) (string, []*RemoteManifest) {
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
