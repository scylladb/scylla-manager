// Copyright (C) 2017 ScyllaDB

package backup

import (
	"sort"

	"github.com/cespare/xxhash"
	"github.com/scylladb/go-set/strset"
	. "github.com/scylladb/scylla-manager/pkg/service/backup/backupspec"
	"github.com/scylladb/scylla-manager/pkg/util/uuid"
)

type fileInfo struct {
	Name string `json:"name"`
	Size int64  `json:"size"`
}

func aggregateManifestInfos(manifests []ManifestInfoWithContent) []ListItem {
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
				for _, fi := range m.Index {
					if fi.Keyspace == u.Keyspace {
						u.Tables.Add(fi.Table)
						ok = true
					}
				}
			}
		}
		if !ok {
			kt := map[string]*strset.Set{}
			for _, fi := range m.Index {
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
			sizes[k] += m.Size
		} else {
			sizes[k] = m.Size
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
