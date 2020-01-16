// Copyright (C) 2017 ScyllaDB

package backup

import (
	"math/rand"
	"strings"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	. "github.com/scylladb/mermaid/pkg/testutils"
	"github.com/scylladb/mermaid/pkg/util/timeutc"
	"github.com/scylladb/mermaid/pkg/util/uuid"
)

func TestRemoteManifestParsePath(t *testing.T) {
	t.Parallel()

	var cmpOpts = cmp.Options{
		UUIDComparer(),
		cmpopts.IgnoreFields(remoteManifest{}, "CleanPath", "Files"),
	}

	golden := remoteManifest{
		ClusterID:   uuid.MustRandom(),
		DC:          "a",
		NodeID:      "b",
		Keyspace:    "c",
		Table:       "d",
		TaskID:      uuid.MustRandom(),
		SnapshotTag: newSnapshotTag(),
		Version:     "f",
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
			Path:  remoteManifestFile(uuid.MustRandom(), uuid.MustRandom(), newSnapshotTag(), "dc", "nodeID", "keysapce", "table", "version") + ".old",
			Error: "expected manifest.json",
		},
		{
			Name:  "sSTable dir",
			Path:  remoteSSTableDir(uuid.MustRandom(), "dc", "nodeID", "keysapce", "table"),
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

	var input []remoteManifest

	// Add product of all the possibilities 2^5 items
	for _, c := range []uuid.UUID{c0, c1} {
		for _, n := range []string{n0, n1} {
			for _, s := range []string{s0, s1} {
				for _, ks := range []string{ks0, ks1} {
					for _, tb := range []string{tb0, tb1} {
						m := remoteManifest{
							ClusterID:   c,
							NodeID:      n,
							SnapshotTag: s,
							Keyspace:    ks,
							Table:       tb,
						}
						input = append(input, m)
					}
				}
			}
		}
	}
	// Add extra items
	input = append(input, remoteManifest{
		ClusterID:   c0,
		SnapshotTag: s3,
		Keyspace:    ks0,
		Table:       tb0,
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
			SnapshotTags: []string{s3},
		},
		{
			ClusterID:    c0,
			Units:        units,
			SnapshotTags: []string{s1, s0},
		},
		{
			ClusterID:    c1,
			Units:        units,
			SnapshotTags: []string{s1, s0},
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
			FilePath: "24101c25a2ae3af787c1b40ee1aca33f/mc-20-big-Summary.db",
			Golden:   "24101c25a2ae3af787c1b40ee1aca33f/mc-20-big",
		},
		{
			Name:     "valid la path",
			FilePath: "24101c25a2ae3af787c1b40ee1aca33f/la-111-big-TOC.db",
			Golden:   "24101c25a2ae3af787c1b40ee1aca33f/la-111-big",
		},
		{
			Name:     "valid ka path",
			FilePath: "24101c25a2ae3af787c1b40ee1aca33f/system_schema-columns-ka-2516-Scylla.db",
			Golden:   "24101c25a2ae3af787c1b40ee1aca33f/system_schema-columns-ka-2516",
		},
		{
			Name:     "invalid path",
			FilePath: "24101c25a2ae3af787c1b40ee1aca33f/invalid-123-file.txt",
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
