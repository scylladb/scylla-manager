// Copyright (C) 2017 ScyllaDB

package backup

import (
	"context"
	"encoding/json"
	"net/http"
	"path"
	"sort"

	"github.com/cespare/xxhash"
	"github.com/pkg/errors"
	"github.com/scylladb/go-log"
	"github.com/scylladb/go-set/strset"
	"github.com/scylladb/scylla-manager/pkg/backup"
	"github.com/scylladb/scylla-manager/pkg/scyllaclient"
	"github.com/scylladb/scylla-manager/pkg/util/uuid"
)

type fileInfo struct {
	Name string `json:"name"`
	Size int64  `json:"size"`
}

func aggregateRemoteManifests(manifests []*backup.RemoteManifest) []ListItem {
	// Group by Snapshot tag
	type key struct {
		ClusterID   uuid.UUID
		SnapshotTag string
	}
	type value struct {
		Keyspace string
		Tables   *strset.Set
	}
	kv := make(map[key][]value)

	for i := range manifests {
		m := manifests[i]
		k := key{m.ClusterID, m.SnapshotTag}
		v, ok := kv[k]
		if ok {
			ok = false
			for _, u := range v {
				for _, fi := range m.Content.Index {
					if fi.Keyspace == u.Keyspace {
						u.Tables.Add(fi.Table)
						ok = true
					}
				}
			}
		}
		if !ok {
			kt := map[string]*strset.Set{}
			for _, fi := range m.Content.Index {
				_, ok := kt[fi.Keyspace]
				if ok {
					kt[fi.Keyspace].Add(fi.Table)
				} else {
					kt[fi.Keyspace] = strset.New(fi.Table)
				}
			}
			for ks, tb := range kt {
				kv[k] = append(kv[k], value{ks, tb})
			}
		}
	}

	// Calculate snapshot total sizes
	sizes := make(map[key]int64)
	for _, m := range manifests {
		k := key{m.ClusterID, m.SnapshotTag}
		_, ok := sizes[k]
		if ok {
			sizes[k] += m.Content.Size
		} else {
			sizes[k] = m.Content.Size
		}
	}

	// Group Snapshot tags by Units
	items := make(map[uint64]*ListItem)
	for k, v := range kv {
		units := make([]Unit, len(v))

		// Generate units from v
		sort.Slice(v, func(i, j int) bool {
			return v[i].Keyspace < v[j].Keyspace // nolint: scopelint
		})
		for i, u := range v {
			units[i] = Unit{
				Keyspace: u.Keyspace,
				Tables:   u.Tables.List(),
			}
			sort.Strings(units[i].Tables)
		}

		// Calculate units hash
		h := hashSortedUnits(k.ClusterID.String(), units)

		l, ok := items[h]
		if !ok {
			l := &ListItem{
				ClusterID: k.ClusterID,
				Units:     units,
				SnapshotInfo: []SnapshotInfo{{
					SnapshotTag: k.SnapshotTag,
					Size:        sizes[k],
				}},
				unitsHash: h,
			}
			items[h] = l
		} else if !l.SnapshotInfo.hasSnapshot(k.SnapshotTag) {
			l.SnapshotInfo = append(l.SnapshotInfo, SnapshotInfo{
				SnapshotTag: k.SnapshotTag,
				Size:        sizes[k],
			})
		}
	}

	// Sort Snapshot tags DESC
	for _, l := range items {
		sort.Slice(l.SnapshotInfo, func(i, j int) bool {
			return l.SnapshotInfo[i].SnapshotTag > l.SnapshotInfo[j].SnapshotTag // nolint: scopelint
		})
	}

	// Convert to list
	var list []ListItem
	for _, l := range items {
		list = append(list, *l)
	}

	// Order by cluster ID and tag for repeatable runs
	sort.Slice(list, func(i, j int) bool {
		if c := uuid.Compare(list[i].ClusterID, list[j].ClusterID); c != 0 {
			return c < 0
		}
		if list[i].SnapshotInfo[0].SnapshotTag != list[j].SnapshotInfo[0].SnapshotTag {
			return list[i].SnapshotInfo[0].SnapshotTag > list[j].SnapshotInfo[0].SnapshotTag
		}
		return list[i].unitsHash < list[j].unitsHash
	})

	return list
}

func hashSortedUnits(marker string, units []Unit) uint64 {
	h := xxhash.New()
	w := func(s string) {
		h.Write([]byte(s))   // nolint: errcheck
		h.Write([]byte{';'}) // nolint: errcheck
	}

	w(marker)
	for _, u := range units {
		w(u.Keyspace)
		for _, t := range u.Tables {
			w(t)
		}
		w("")
	}

	return h.Sum64()
}

type manifestHelper interface {
	ListManifests(ctx context.Context, f ListFilter) ([]*backup.RemoteManifest, error)
	DeleteManifest(ctx context.Context, m *backup.RemoteManifest) error
}

// multiVersionManifestLister allows to list manifests depending on bucket metadata
// version. It looks up version of metadata by reading version file from location,
// and lists manifests matching it.
// In case when filter doesn't have enough information to determine version, all
// manifests available in location are listed.
type multiVersionManifestLister struct {
	host     string
	location backup.Location
	client   *scyllaclient.Client
	helpers  map[string]manifestHelper
}

func newMultiVersionManifestLister(host string, location backup.Location, client *scyllaclient.Client, logger log.Logger) *multiVersionManifestLister {
	return &multiVersionManifestLister{
		host:     host,
		location: location,
		client:   client,
		helpers: map[string]manifestHelper{
			"v1": newManifestV1Helper(host, location, client, logger),
			"v2": newManifestV2Helper(host, location, client, logger),
		},
	}
}

func (l *multiVersionManifestLister) ListManifests(ctx context.Context, f ListFilter) ([]*backup.RemoteManifest, error) {
	if f.ClusterID != uuid.Nil && f.DC != "" && f.NodeID != "" {
		version, err := getMetadataVersion(ctx, l.host, l.location, l.client, f.ClusterID, f.DC, f.NodeID)
		if err != nil {
			return nil, err
		}

		lister, ok := l.helpers[version]
		if !ok {
			return nil, errors.Errorf("not supported metadata version: %s", version)
		}
		return lister.ListManifests(ctx, f)
	}

	// Group manifests.
	type key struct {
		DC          string
		ClusterID   uuid.UUID
		NodeID      string
		TaskID      uuid.UUID
		SnapshotTag string
	}

	manifests := make(map[key][]*backup.RemoteManifest)

	for _, lister := range l.helpers {
		ms, err := lister.ListManifests(ctx, f)
		if err != nil {
			return nil, err
		}

		for _, m := range ms {
			k := key{m.DC, m.ClusterID, m.NodeID, m.TaskID, m.SnapshotTag}
			manifests[k] = append(manifests[k], m)
		}
	}

	var out []*backup.RemoteManifest
	for k := range manifests {
		out = append(out, l.removeDuplicates(manifests[k])...)
	}

	// Sort for repeatable listing.
	sort.Slice(out, func(i, j int) bool {
		return path.Join(out[i].CleanPath...) < path.Join(out[j].CleanPath...)
	})

	return out, nil
}

// removeDuplicates scans list of manifests and returns only manifests of the
// single version with preference for v2.
// Only manifests from the same node should be provided.
func (l *multiVersionManifestLister) removeDuplicates(ms []*backup.RemoteManifest) []*backup.RemoteManifest {
	if len(ms) <= 1 {
		return ms
	}
	var (
		// Migrated manifests have both v1 and v2 manifests present after
		// migration but v2 manifest has Content.Version set to v1 because of
		// mechanism used in purging.
		// Here we are using length of the CleanPath as a signal for
		// distinguishing between v1 and v2 because v1 has longer path.
		pathLength        = len(ms[0].CleanPath)
		v2CleanPathLength = v2CleanPathLength()
	)

	for i := range ms {
		if len(ms[i].CleanPath) != pathLength {
			for j := range ms {
				if len(ms[j].CleanPath) == v2CleanPathLength {
					// There should be only one manifest per node in v2.
					return []*backup.RemoteManifest{ms[j]}
				}
			}
		}
	}

	return ms
}

// v2CleanPathLength uses dummy data to parse v2 path and return length of the
// clean path for the v2 manifest.
func v2CleanPathLength() int {
	m := backup.RemoteManifest{}
	if err := m.ParsePartialPath(backup.RemoteManifestFile(
		uuid.NewTime(), uuid.NewTime(), "sm_20091110230000UTC", "b", "c",
	)); err != nil {
		panic(err)
	}

	return len(m.CleanPath)
}

func getMetadataVersion(ctx context.Context, host string, location backup.Location, client *scyllaclient.Client,
	clusterID uuid.UUID, dc, nodeID string) (string, error) {
	p := location.RemotePath(backup.RemoteMetaVersionFile(clusterID, dc, nodeID))
	content, err := client.RcloneCat(ctx, host, p)
	if err != nil {
		if scyllaclient.StatusCodeOf(err) == http.StatusNotFound {
			// means V1, since we introduced this file in V2
			return "v1", nil
		}
		return "", err
	}

	var mv struct {
		Version string `json:"version"`
	}
	if err := json.Unmarshal(content, &mv); err != nil {
		return "", err
	}

	return mv.Version, nil
}
