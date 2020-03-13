// Copyright (C) 2017 ScyllaDB

package backup

import (
	"context"
	"encoding/json"
	"io/ioutil"
	"math/rand"
	"path"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	"github.com/scylladb/go-log"
	"github.com/scylladb/mermaid/pkg/scyllaclient/scyllaclienttest"
	. "github.com/scylladb/mermaid/pkg/testutils"
	"github.com/scylladb/mermaid/pkg/util/timeutc"
	"github.com/scylladb/mermaid/pkg/util/uuid"
)

func TestRemoteManifestParsePath(t *testing.T) {
	t.Parallel()

	var cmpOpts = cmp.Options{
		UUIDComparer(),
		cmpopts.IgnoreFields(remoteManifest{}, "CleanPath"),
		cmpopts.IgnoreUnexported(remoteManifest{}),
	}

	golden := remoteManifest{
		ClusterID:   uuid.MustRandom(),
		DC:          "a",
		NodeID:      "b",
		TaskID:      uuid.MustRandom(),
		SnapshotTag: newSnapshotTag(),
	}

	var m remoteManifest
	if err := m.ParsePartialPath(golden.RemoteManifestFile()); err != nil {
		t.Fatal("ParsePartialPath() error", err)
	}
	if diff := cmp.Diff(m, golden, cmpOpts); diff != "" {
		t.Fatal("ParsePartialPath() diff", diff)
	}
}

func TestRemoteManifestParsePathEmpty(t *testing.T) {
	t.Parallel()

	table := []struct {
		Name string
		Path string
	}{
		{
			Name: "empty",
			Path: "",
		},
		{
			Name: "backup prefix",
			Path: "backup",
		},
		{
			Name: "backup prefix with slash",
			Path: "/backup",
		},
	}

	for i := range table {
		test := table[i]

		t.Run(test.Name, func(t *testing.T) {
			t.Parallel()
			var p remoteManifest
			if err := p.ParsePartialPath(test.Path); err != nil {
				t.Fatal("ParsePartialPath() error", err)
			}
		})
	}
}

func TestRemoteManifestParsePathErrors(t *testing.T) {
	t.Parallel()

	table := []struct {
		Name  string
		Path  string
		Error string
	}{
		{
			Name:  "invalid prefix",
			Path:  "foobar",
			Error: "expected backup",
		},
		{
			Name:  "invalid cluster ID",
			Path:  "backup/meta/cluster/bla",
			Error: "invalid UUID",
		},
		{
			Name:  "invalid static DC",
			Path:  "backup/meta/cluster/" + uuid.MustRandom().String() + "/bla",
			Error: "expected dc",
		},
		{
			Name:  "not a manifest file",
			Path:  remoteManifestFile(uuid.MustRandom(), uuid.MustRandom(), newSnapshotTag(), "dc", "nodeID") + ".old",
			Error: "expected manifest.json",
		},
		{
			Name:  "sSTable dir",
			Path:  remoteSSTableVersionDir(uuid.MustRandom(), "dc", "nodeID", "keyspace", "table", "version"),
			Error: "expected meta",
		},
	}

	for i := range table {
		test := table[i]

		t.Run(test.Name, func(t *testing.T) {
			t.Parallel()

			var m remoteManifest
			err := m.ParsePartialPath(test.Path)
			if err == nil {
				t.Fatal("ParsePartialPath() expected error")
			}

			t.Log("ParsePartialPath():", err)
			if !strings.Contains(err.Error(), test.Error) {
				t.Fatalf("ParsePartialPath() = %v, expected %v", err, test.Error)
			}
		})
	}
}

var listItemCmpOpts = cmp.Options{
	UUIDComparer(),
	cmpopts.IgnoreUnexported(ListItem{}),
}

func TestAggregateRemoteManifests(t *testing.T) {
	t.Parallel()

	c0 := uuid.NewTime()
	c1 := uuid.NewTime()

	n0 := "node0"
	n1 := "node1"

	now := timeutc.Now()
	s0 := snapshotTagAt(now.Add(1 * time.Hour))
	s1 := snapshotTagAt(now.Add(2 * time.Hour))
	s3 := snapshotTagAt(now.Add(3 * time.Hour))

	ks0 := "keyspace0"
	ks1 := "keyspace1"

	tb0 := "table0"
	tb1 := "table1"

	manifestSize := int64(1024)
	nodeCount := int64(2)

	var input []*remoteManifest

	// Add product of all the possibilities 2^5 items
	for _, c := range []uuid.UUID{c0, c1} {
		for _, n := range []string{n0, n1} {
			for _, s := range []string{s0, s1} {
				var idx []filesInfo
				for _, ks := range []string{ks0, ks1} {
					for _, tb := range []string{tb0, tb1} {
						idx = append(idx, filesInfo{
							Keyspace: ks,
							Table:    tb,
						})
					}
				}

				m := &remoteManifest{
					ClusterID:   c,
					NodeID:      n,
					SnapshotTag: s,
					Content: manifestContent{
						Version: "v2",
						Index:   idx,
						Size:    manifestSize,
					},
				}
				input = append(input, m)
			}
		}
	}
	// Add extra items
	input = append(input, &remoteManifest{
		ClusterID:   c0,
		SnapshotTag: s3,
		Content: manifestContent{
			Version: "v2",
			Index: []filesInfo{
				{
					Keyspace: ks0,
					Table:    tb0,
				},
			},
			Size: manifestSize,
		},
	})
	// Shuffle items
	rand.Shuffle(len(input), func(i, j int) {
		tmp := input[i]
		input[i] = input[j]
		input[j] = tmp
	})

	units := []Unit{
		{
			Keyspace: ks0,
			Tables:   []string{tb0, tb1},
		},
		{
			Keyspace: ks1,
			Tables:   []string{tb0, tb1},
		},
	}

	golden := []ListItem{
		{
			ClusterID:    c0,
			Units:        []Unit{{Keyspace: ks0, Tables: []string{tb0}}},
			SnapshotInfo: []SnapshotInfo{{SnapshotTag: s3, Size: manifestSize}},
		},
		{
			ClusterID: c0,
			Units:     units,
			SnapshotInfo: []SnapshotInfo{
				{SnapshotTag: s1, Size: nodeCount * manifestSize},
				{SnapshotTag: s0, Size: nodeCount * manifestSize},
			},
		},
		{
			ClusterID: c1,
			Units:     units,
			SnapshotInfo: []SnapshotInfo{
				{SnapshotTag: s1, Size: nodeCount * manifestSize},
				{SnapshotTag: s0, Size: nodeCount * manifestSize},
			},
		},
	}
	v := aggregateRemoteManifests(input)
	if diff := cmp.Diff(v, golden, listItemCmpOpts); diff != "" {
		t.Error("AggregateRemoteManifests() diff", diff)
	}
}

func TestGroupingKey(t *testing.T) {
	table := []struct {
		Name     string
		FilePath string
		Golden   string
		Error    bool
	}{
		{
			Name:     "valid mc path",
			FilePath: "keyspace/my_keyspace/table/my_table/24101c25a2ae3af787c1b40ee1aca33f/mc-20-big-Summary.db",
			Golden:   "keyspace/my_keyspace/table/my_table/24101c25a2ae3af787c1b40ee1aca33f/mc-20-big",
		},
		{
			Name:     "valid crc32 path",
			FilePath: "keyspace/my_keyspace/table/my_table/24101c25a2ae3af787c1b40ee1aca33f/mc-20-big-Digest.crc32",
			Golden:   "keyspace/my_keyspace/table/my_table/24101c25a2ae3af787c1b40ee1aca33f/mc-20-big",
		},
		{
			Name:     "valid la path",
			FilePath: "keyspace/my_keyspace/table/my_table/24101c25a2ae3af787c1b40ee1aca33f/la-111-big-TOC.db",
			Golden:   "keyspace/my_keyspace/table/my_table/24101c25a2ae3af787c1b40ee1aca33f/la-111-big",
		},
		{
			Name:     "valid ka path",
			FilePath: "keyspace/my_keyspace/table/my_table/24101c25a2ae3af787c1b40ee1aca33f/system_schema-columns-ka-2516-Scylla.db",
			Golden:   "keyspace/my_keyspace/table/my_table/24101c25a2ae3af787c1b40ee1aca33f/system_schema-columns-ka-2516",
		},
		{
			Name:     "valid manifest path",
			FilePath: "keyspace/my_keyspace/table/my_table/24101c25a2ae3af787c1b40ee1aca33f/manifest.json",
			Golden:   "keyspace/my_keyspace/table/my_table/24101c25a2ae3af787c1b40ee1aca33f/manifest.json",
		},
		{
			Name:     "invalid path",
			FilePath: "keyspace/my_keyspace/table/my_table/24101c25a2ae3af787c1b40ee1aca33f/invalid-123-file.txt",
			Error:    true,
		},
	}

	for i := range table {
		test := table[i]
		t.Run(test.Name, func(t *testing.T) {
			key, err := groupingKey(test.FilePath)
			if test.Error && err == nil {
				t.Fatal("groupingKey()=nil, expected error")
			} else if !test.Error && err != nil {
				t.Fatalf("groupingKey()= %+v", err)
			}
			if key != test.Golden {
				t.Fatalf("groupingKey()=%v, expected %v", key, test.Golden)
			}
		})
	}
}

func TestListManifests(t *testing.T) {
	t.Parallel()

	ts := []struct {
		Name       string
		Location   Location
		GoldenFile string
		Filter     ListFilter
	}{
		{
			Name:       "Smoke manifest listing",
			Location:   Location{Provider: "walker", Path: "list"},
			GoldenFile: "testdata/walker/list/golden.json",
		},
		{
			Name:       "Support for v1 and v2 manifest at once",
			Location:   Location{Provider: "walker", Path: "v1-support"},
			GoldenFile: "testdata/walker/v1-support/golden.json",
		},
		{
			Name:       "List only manifests from metadata version file",
			Location:   Location{Provider: "walker", Path: "version-file"},
			GoldenFile: "testdata/walker/version-file/golden.json",
			Filter: ListFilter{
				ClusterID: uuid.MustParse("45e7257a-fe1d-439b-9759-918f34abf83c"),
				DC:        "dc1",
				NodeID:    "49f5a202-6661-4a1e-a674-4c7b97247fdb",
			},
		},
		{
			Name:       "Manifest contains file sizes",
			Location:   Location{Provider: "walker", Path: "file-size"},
			GoldenFile: "testdata/walker/file-size/golden.json",
		},
	}

	for i := range ts {
		test := ts[i]
		t.Run(test.Name, func(t *testing.T) {
			t.Parallel()

			client, closeServer := scyllaclienttest.NewFakeRcloneServer(t, scyllaclienttest.PathFileMatcher("/metrics", "testdata/walker/scylla_metrics/metrics"))
			defer closeServer()

			mr := newMultiVersionManifestLister(scyllaclienttest.TestHost, test.Location, client, log.NewDevelopment())

			manifests, err := mr.ListManifests(context.Background(), test.Filter)
			if err != nil {
				t.Fatal("listManifests() error", err)
			}

			// Sort for repeatable runs
			sort.Slice(manifests, func(i, j int) bool {
				return path.Join(manifests[i].CleanPath...) < path.Join(manifests[j].CleanPath...)
			})

			if UpdateGoldenFiles() {
				b, err := json.Marshal(manifests)
				if err != nil {
					t.Fatal(err)
				}
				if err := ioutil.WriteFile(test.GoldenFile, b, 0666); err != nil {
					t.Error(err)
				}
			}

			b, err := ioutil.ReadFile(test.GoldenFile)
			if err != nil {
				t.Fatal(err)
			}
			var golden []*remoteManifest
			if err := json.Unmarshal(b, &golden); err != nil {
				t.Fatal(err)
			}

			opts := []cmp.Option{
				UUIDComparer(), cmpopts.IgnoreUnexported(remoteManifest{}),
			}
			if diff := cmp.Diff(manifests, golden, opts...); diff != "" {
				t.Fatalf("listManifests() = %v, diff %s", manifests, diff)
			}
		})
	}
}
