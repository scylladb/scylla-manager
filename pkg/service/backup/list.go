// Copyright (C) 2017 ScyllaDB

package backup

import (
	"context"
	"path"
	"sync"
	"time"

	"github.com/scylladb/scylla-manager/pkg/scyllaclient"
	. "github.com/scylladb/scylla-manager/pkg/service/backup/backupspec"
	"github.com/scylladb/scylla-manager/pkg/util/inexlist/ksfilter"
	"github.com/scylladb/scylla-manager/pkg/util/uuid"
)

// listManifestsInAllLocations returns manifests for all nodes of a in all
// locations specified in hosts.
func listManifestsInAllLocations(ctx context.Context, client *scyllaclient.Client, hosts []hostInfo, clusterID uuid.UUID) ([]*ManifestInfo, error) {
	var (
		locations = make(map[Location]struct{})
		manifests []*ManifestInfo
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
func listManifests(ctx context.Context, client *scyllaclient.Client, host string, location Location, clusterID uuid.UUID) ([]*ManifestInfo, error) {
	baseDir := RemoteMetaClusterDCDir(clusterID)
	if clusterID == uuid.Nil {
		baseDir = path.Join("backup", string(MetaDirKind))
	}

	opts := scyllaclient.RcloneListDirOpts{
		FilesOnly: true,
		Recurse:   true,
	}

	var manifests []*ManifestInfo
	err := client.RcloneListDirIter(ctx, host, location.RemotePath(baseDir), &opts, func(f *scyllaclient.RcloneListDirItem) {
		p := path.Join(baseDir, f.Path)
		m := &ManifestInfo{}
		if err := m.ParsePath(p); err != nil {
			return
		}
		m.Location = location
		manifests = append(manifests, m)
	})
	if err != nil {
		return nil, err
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

func (f *ListFilter) prune(m *ManifestInfo) bool {
	filters := []func(m *ManifestInfo) bool{
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

func (f *ListFilter) pruneClusterID(m *ManifestInfo) bool {
	if m.ClusterID != uuid.Nil && f.ClusterID != uuid.Nil {
		if m.ClusterID != f.ClusterID {
			return true
		}
	}
	return false
}

func (f *ListFilter) pruneDC(m *ManifestInfo) bool {
	if m.DC != "" && f.DC != "" {
		if m.DC != f.DC {
			return true
		}
	}
	return false
}

func (f *ListFilter) pruneNodeID(m *ManifestInfo) bool {
	if m.NodeID != "" && f.NodeID != "" {
		if m.NodeID != f.NodeID {
			return true
		}
	}
	return false
}

func (f *ListFilter) pruneTaskID(m *ManifestInfo) bool {
	if m.TaskID != uuid.Nil && f.TaskID != uuid.Nil {
		if m.TaskID != f.TaskID {
			return true
		}
	}
	return false
}

func (f *ListFilter) pruneSnapshotTag(m *ManifestInfo) bool {
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

func (f *ListFilter) pruneTemporary(m *ManifestInfo) bool {
	if m.Temporary {
		return !f.Temporary
	}
	return false
}

func filterManifests(manifests []*ManifestInfo, filter ListFilter) []*ManifestInfo {
	var out []*ManifestInfo
	for _, m := range manifests {
		if !filter.prune(m) {
			out = append(out, m)
		}
	}
	return out
}

func filterManifestIndex(c *ManifestContent, ksf *ksfilter.Filter) {
	if len(ksf.Filters()) == 0 {
		return
	}
	var index []FilesMeta
	for _, u := range c.Index {
		if ksf.Check(u.Keyspace, u.Table) {
			index = append(index, u)
		}
	}
	c.Index = index
}

func groupManifestsByNode(manifests []*ManifestInfo) map[string][]*ManifestInfo {
	v := map[string][]*ManifestInfo{}
	for _, m := range manifests {
		v[m.NodeID] = append(v[m.NodeID], m)
	}
	return v
}

func groupManifestsByTask(manifests []*ManifestInfo) map[uuid.UUID][]*ManifestInfo {
	v := map[uuid.UUID][]*ManifestInfo{}
	for _, m := range manifests {
		v[m.TaskID] = append(v[m.TaskID], m)
	}
	return v
}

// popNodeIDManifestsForLocation returns a function that for a given location
// finds next node and it's manifests from that location.
func popNodeIDManifestsForLocation(manifests []*ManifestInfo) func(h hostInfo) (string, []*ManifestInfo) {
	var mu sync.Mutex
	nodeIDManifests := groupManifestsByNode(manifests)
	return func(h hostInfo) (string, []*ManifestInfo) {
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
