// Copyright (C) 2017 ScyllaDB

package backup

import (
	"path"
	"sort"
	"strings"

	"github.com/cespare/xxhash"
	"github.com/pkg/errors"
	"github.com/scylladb/go-set/strset"
	"github.com/scylladb/mermaid/uuid"
)

const manifestFileSuffix = "-Data.db"

type remoteManifest struct {
	CleanPath []string

	ClusterID   uuid.UUID
	DC          string
	NodeID      string
	Keyspace    string
	Table       string
	TaskID      uuid.UUID
	SnapshotTag string
	Version     string

	// Files contained in manifest, requires loading them.
	Files []string
}

// ParsePartialPath tries extracting properties from remote path to manifest.
// This is a reverse process to calling RemoteManifestFile function.
// It supports path prefixes i.e. paths that may lead to a manifest file,
// in that case no error is returned but only some fields will be set.
func (m *remoteManifest) ParsePartialPath(s string) error {
	// Clear values
	*m = remoteManifest{}

	// Ignore empty strings
	if s == "" {
		return nil
	}

	static := func(s string) func(v string) error {
		return func(v string) error {
			if v != s {
				return errors.Errorf("expected %s got %s", s, v)
			}
			return nil
		}
	}

	id := func(ptr *uuid.UUID) func(v string) error {
		return func(v string) error {
			return ptr.UnmarshalText([]byte(v))
		}
	}

	str := func(ptr *string) func(v string) error {
		return func(v string) error {
			*ptr = v
			return nil
		}
	}

	parsers := []func(v string) error{
		static("backup"),
		static("meta"),
		static("cluster"),
		id(&m.ClusterID),
		static("dc"),
		str(&m.DC),
		static("node"),
		str(&m.NodeID),
		static("keyspace"),
		str(&m.Keyspace),
		static("table"),
		str(&m.Table),
		static("task"),
		id(&m.TaskID),
		static("tag"),
		func(v string) error {
			if !isSnapshotTag(v) {
				return errors.Errorf("invalid snapshot tag %s", v)
			}
			m.SnapshotTag = v
			return nil
		},
		str(&m.Version),
		static(manifest),
	}

	// Clean path for usage with strings.Split
	s = strings.TrimPrefix(path.Clean(s), sep)
	// Set partial clean path
	m.CleanPath = strings.Split(s, sep)

	// Parse fields
	for i, v := range m.CleanPath {
		if i >= len(parsers) {
			return nil
		}
		if err := parsers[i](v); err != nil {
			return errors.Wrapf(err, "invalid path element at position %d", i)
		}
	}

	return nil
}

func (m remoteManifest) RemoteManifestFile() string {
	return remoteManifestFile(m.ClusterID, m.TaskID, m.SnapshotTag, m.DC, m.NodeID, m.Keyspace, m.Table, m.Version)
}

func aggregateRemoteManifests(manifests []remoteManifest) []ListItem {
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

	for _, m := range manifests {
		v, ok := kv[key{m.ClusterID, m.SnapshotTag}]
		if ok {
			ok = false
			for _, u := range v {
				if u.Keyspace == m.Keyspace {
					u.Tables.Add(m.Table)
					ok = true
					break
				}
			}
		}
		if !ok {
			kv[key{m.ClusterID, m.SnapshotTag}] = append(v, value{m.Keyspace, strset.New(m.Table)})
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
				ClusterID:    k.ClusterID,
				Units:        units,
				unitsHash:    h,
				SnapshotTags: []string{k.SnapshotTag},
			}
			items[h] = l
		} else if !sliceContains(k.SnapshotTag, l.SnapshotTags) {
			l.SnapshotTags = append(l.SnapshotTags, k.SnapshotTag)
		}
	}

	// Sort Snapshot tags DESC
	for _, l := range items {
		sort.Slice(l.SnapshotTags, func(i, j int) bool {
			return l.SnapshotTags[i] > l.SnapshotTags[j] // nolint: scopelint
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
		if list[i].SnapshotTags[0] != list[j].SnapshotTags[0] {
			return list[i].SnapshotTags[0] > list[j].SnapshotTags[0]
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
