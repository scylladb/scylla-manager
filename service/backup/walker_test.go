// Copyright (C) 2017 ScyllaDB

package backup

import (
	"context"
	"encoding/json"
	"io/ioutil"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/scylladb/mermaid/internal/timeutc"
	. "github.com/scylladb/mermaid/mermaidtest"
	"github.com/scylladb/mermaid/scyllaclient/scyllaclienttest"
	"github.com/scylladb/mermaid/uuid"
)

func TestWalkerSimple(t *testing.T) {
	t.Parallel()

	table := []struct {
		Name  string
		Level int
		Files []string
	}{
		{
			Name:  "level 0",
			Level: 0,
		},
		{
			Name:  "level 1",
			Level: 1,
			Files: []string{"file.txt"},
		},
		{
			Name:  "level 3",
			Level: 3,
			Files: []string{"a/b/b.txt", "c/d/d.txt"},
		},
	}

	client, cl := scyllaclienttest.NewFakeRcloneServer(t)
	defer cl()

	w := walker{
		Location: Location{Provider: "walkertest"},
		Client:   client,
	}

	t.Run("group", func(t *testing.T) {
		for i := range table {
			test := table[i]

			t.Run(test.Name, func(t *testing.T) {
				t.Parallel()

				files, err := w.FilesAtLevel(context.Background(), scyllaclienttest.TestHost, test.Level)
				if err != nil {
					t.Fatal("FilesAtLevel() error", err)
				}
				if diff := cmp.Diff(files, test.Files); diff != "" {
					t.Fatal("FilesAtLevel() unexpected files got", files, "expected", test.Files)
				}
			})
		}
	})
}

func TestWalkerPrune(t *testing.T) {
	t.Parallel()

	client, cl := scyllaclienttest.NewFakeRcloneServer(t)
	defer cl()

	w := walker{
		Location: Location{Provider: "walkertest"},
		Client:   client,
		Prune: func(dir string) bool {
			return dir == "c"
		},
	}

	var (
		level  = 3
		golden = []string{"a/b/b.txt"}
	)

	files, err := w.FilesAtLevel(context.Background(), scyllaclienttest.TestHost, level)
	if err != nil {
		t.Fatal("FilesAtLevel() error", err)
	}
	if diff := cmp.Diff(files, golden); diff != "" {
		t.Fatal("FilesAtLevel() unexpected files got", files, "expected", golden)
	}
}

func TestListFilterPruneFunc(t *testing.T) {
	t.Parallel()

	dir := remoteManifestFile(uuid.MustRandom(), uuid.MustRandom(), newSnapshotTag(), "dc", "nodeID", "keysapce", "table", "version")

	table := []struct {
		Name   string
		Filter ListFilter
		Dir    string
		Prune  bool
	}{
		{
			Name:  "Empty filer valid dir",
			Dir:   "backup",
			Prune: false,
		},
		{
			Name:  "Empty filer invalid dir",
			Dir:   "foobar",
			Prune: true,
		},
		{
			Name:   "Filter cluster",
			Filter: ListFilter{ClusterID: uuid.MustRandom()},
			Dir:    dir,
			Prune:  true,
		},
		{
			Name:   "Filter keysapce",
			Filter: ListFilter{Keyspace: []string{"keysapce2"}},
			Dir:    dir,
			Prune:  true,
		},
		{
			Name:   "Filter keysapce",
			Filter: ListFilter{Keyspace: []string{"keysapce.table2"}},
			Dir:    dir,
			Prune:  true,
		},
		{
			Name:   "Filter keysapce",
			Filter: ListFilter{Keyspace: []string{"keysapce.table2"}},
			Dir:    dir,
			Prune:  true,
		},
		{
			Name:   "Filter min date",
			Filter: ListFilter{MinDate: timeutc.Now().Add(time.Hour)},
			Dir:    dir,
			Prune:  true,
		},
		{
			Name:   "Filter max date",
			Filter: ListFilter{MaxDate: timeutc.Now().Add(-time.Hour)},
			Dir:    dir,
			Prune:  true,
		},
	}

	for i := range table {
		test := table[i]

		t.Run(test.Name, func(t *testing.T) {
			t.Parallel()

			f, err := makeListFilterPruneFunc(test.Filter)
			if err != nil {
				t.Fatal(err)
			}
			if f(test.Dir) != test.Prune {
				t.Errorf("ListFilterPruneFunc() got %v expected %v", f(test.Dir), test.Prune)
			}
		})
	}
}

func TestListManifests(t *testing.T) {
	t.Parallel()

	const goldenFile = "testdata/walker/list/golden.json"

	client, cl := scyllaclienttest.NewFakeRcloneServer(t)
	defer cl()

	manifests, err := listManifests(context.Background(),
		Location{Provider: "listtest"},
		client,
		scyllaclienttest.TestHost,
		ListFilter{},
		true,
	)
	if err != nil {
		t.Fatal("listManifests() error", err)
	}

	if UpdateGoldenFiles() {
		b, err := json.Marshal(manifests)
		if err != nil {
			t.Fatal(err)
		}
		if err := ioutil.WriteFile(goldenFile, b, 0666); err != nil {
			t.Error(err)
		}
	}

	b, err := ioutil.ReadFile(goldenFile)
	if err != nil {
		t.Fatal(err)
	}
	var golden []remoteManifest
	if err := json.Unmarshal(b, &golden); err != nil {
		t.Fatal(err)
	}

	if diff := cmp.Diff(manifests, golden, remoteManifestCmpOpts); diff != "" {
		t.Fatal("listManifests() unexpected files got", manifests, "expected", golden)
	}
}
