// Copyright (C) 2017 ScyllaDB

package backup

import (
	"compress/gzip"
	"context"
	"encoding/json"
	"io"
	"path"
	"sort"
	"strings"

	"github.com/cespare/xxhash"
	"github.com/pkg/errors"
	"github.com/scylladb/go-log"
	"github.com/scylladb/go-set/strset"
	"github.com/scylladb/mermaid/pkg/scyllaclient"
	"github.com/scylladb/mermaid/pkg/util/pathparser"
	"github.com/scylladb/mermaid/pkg/util/uuid"
)

type filesInfo struct {
	Keyspace string   `json:"keyspace"`
	Table    string   `json:"table"`
	Version  string   `json:"version"`
	Files    []string `json:"files"`
}

type manifestContent struct {
	Version string      `json:"version"`
	Index   []filesInfo `json:"index"`
}

func (m *manifestContent) Read(r io.Reader) error {
	gr, err := gzip.NewReader(r)
	if err != nil {
		return err
	}

	if err := json.NewDecoder(gr).Decode(m); err != nil {
		return err
	}
	return gr.Close()
}

func (m *manifestContent) Write(w io.Writer) error {
	gw := gzip.NewWriter(w)

	if err := json.NewEncoder(gw).Encode(m); err != nil {
		return err
	}

	return gw.Close()
}

type remoteManifest struct {
	CleanPath []string

	Location    Location
	DC          string
	ClusterID   uuid.UUID
	NodeID      string
	TaskID      uuid.UUID
	SnapshotTag string
	Content     manifestContent
}

func (m *remoteManifest) RemoteManifestFile() string {
	return remoteManifestFile(m.ClusterID, m.TaskID, m.SnapshotTag, m.DC, m.NodeID)
}

func (m *remoteManifest) RemoteSSTableVersionDir(keyspace, table, version string) string {
	return remoteSSTableVersionDir(m.ClusterID, m.DC, m.NodeID, keyspace, table, version)
}

func (m *remoteManifest) ReadContent(r io.Reader) error {
	return m.Content.Read(r)
}

func (m *remoteManifest) DumpContent(w io.Writer) error {
	return m.Content.Write(w)
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

	// Clean path for usage with strings.Split
	s = strings.TrimPrefix(path.Clean(s), sep)

	// Set partial clean path
	m.CleanPath = strings.Split(s, sep)

	flatParser := func(v string) error {
		p := pathparser.New(v, "_")

		return p.Parse(
			pathparser.Static("task"),
			pathparser.ID(&m.TaskID),
			pathparser.Static("tag"),
			pathparser.Static("sm"),
			func(v string) error {
				tag := "sm_" + v
				if !isSnapshotTag(tag) {
					return errors.Errorf("invalid snapshot tag %s", tag)
				}
				m.SnapshotTag = tag
				return nil
			},
			pathparser.Static(manifest),
		)
	}

	p := pathparser.New(s, sep)
	err := p.Parse(
		pathparser.Static("backup"),
		pathparser.Static("meta"),
		pathparser.Static("cluster"),
		pathparser.ID(&m.ClusterID),
		pathparser.Static("dc"),
		pathparser.String(&m.DC),
		pathparser.Static("node"),
		pathparser.String(&m.NodeID),
		flatParser,
	)
	if err != nil {
		return err
	}

	return nil
}

func aggregateRemoteManifests(manifests []*remoteManifest) []ListItem {
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
				SnapshotTags: []string{k.SnapshotTag},
				unitsHash:    h,
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

const manifestFileSuffix = "-Data.db"

type manifestHelper interface {
	ListManifests(ctx context.Context, f ListFilter) ([]*remoteManifest, error)
	DeleteManifest(ctx context.Context, m *remoteManifest) error
}

type multiManifestHelper struct {
	helpers map[string]manifestHelper
}

func newMultiManifestHelper(host string, location Location, client *scyllaclient.Client,
	logger log.Logger) manifestHelper {
	return &multiManifestHelper{
		helpers: map[string]manifestHelper{
			"v1": newManifestV1Helper(host, location, client, logger.Named("v1")),
			"v2": newManifestV2Helper(host, location, client, logger.Named("v2")),
		}}
}

func (m *multiManifestHelper) ListManifests(ctx context.Context, f ListFilter) ([]*remoteManifest, error) {
	var manifests []*remoteManifest
	for _, hl := range m.helpers {
		m, err := hl.ListManifests(ctx, f)
		if err != nil {
			return nil, err
		}
		manifests = append(manifests, m...)
	}
	return manifests, nil
}

func (m *multiManifestHelper) DeleteManifest(ctx context.Context, rm *remoteManifest) error {
	h, ok := m.helpers[rm.Content.Version]
	if !ok {
		return errors.Errorf("unsupported manifest version: %s", rm.Content.Version)
	}
	return h.DeleteManifest(ctx, rm)
}
