// Copyright (C) 2017 ScyllaDB

package backup

import (
	"context"
	"path"
	"sync"

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
