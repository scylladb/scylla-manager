// Copyright (C) 2017 ScyllaDB

package backup

import (
	"context"
	"encoding/json"
	"io/ioutil"
	"path"
	"sort"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/scylladb/go-log"
	"github.com/scylladb/mermaid/internal/timeutc"
	. "github.com/scylladb/mermaid/mermaidtest"
	"github.com/scylladb/mermaid/scyllaclient/scyllaclienttest"
	"github.com/scylladb/mermaid/uuid"
)

func TestWalkerDirsAtLevelN(t *testing.T) {
	t.Parallel()

	table := []struct {
		Name  string
		Level int
		Dirs  []string
	}{
		{
			Name:  "level 0",
			Level: 0,
		},
		{
			Name:  "level 1",
			Level: 1,
			Dirs:  []string{"a", "c"},
		},
		{
			Name:  "level 2",
			Level: 2,
			Dirs:  []string{"a/b", "c/d"},
		},
	}

	client, cl := scyllaclienttest.NewFakeRcloneServer(t)
	defer cl()

	w := walker{
		Host:     scyllaclienttest.TestHost,
		Location: Location{Provider: "walkertest"},
		Client:   client,
	}

	t.Run("group", func(t *testing.T) {
		for i := range table {
			test := table[i]

			t.Run(test.Name, func(t *testing.T) {
				t.Parallel()

				files, err := w.DirsAtLevelN(context.Background(), "", test.Level)
				if err != nil {
					t.Fatal("DirsAtLevelN() error", err)
				}
				if diff := cmp.Diff(files, test.Dirs); diff != "" {
					t.Fatalf("DirsAtLevelN() = %v, expected %v", files, test.Dirs)
				}
			})
		}
	})
}

func TestWalkerDirsAtLevelNPrune(t *testing.T) {
	t.Parallel()

	client, cl := scyllaclienttest.NewFakeRcloneServer(t)
	defer cl()

	w := walker{
		Host:     scyllaclienttest.TestHost,
		Location: Location{Provider: "walkertest"},
		Client:   client,
		Prune: func(dir string) bool {
			return dir == "c"
		},
	}

	var (
		level  = 2
		golden = []string{"a/b"}
	)

	files, err := w.DirsAtLevelN(context.Background(), "", level)
	if err != nil {
		t.Fatal("DirsAtLevelN() error", err)
	}
	if diff := cmp.Diff(files, golden); diff != "" {
		t.Fatalf("DirsAtLevelN() = %v, expected %v", files, golden)
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
			Name:  "empty filer valid dir",
			Dir:   "backup",
			Prune: false,
		},
		{
			Name:  "empty filer invalid dir",
			Dir:   "foobar",
			Prune: true,
		},
		{
			Name:   "filter cluster",
			Filter: ListFilter{ClusterID: uuid.MustRandom()},
			Dir:    dir,
			Prune:  true,
		},
		{
			Name:   "filter keysapce",
			Filter: ListFilter{Keyspace: []string{"keysapce2"}},
			Dir:    dir,
			Prune:  true,
		},
		{
			Name:   "filter keysapce",
			Filter: ListFilter{Keyspace: []string{"keysapce.table2"}},
			Dir:    dir,
			Prune:  true,
		},
		{
			Name:   "filter keysapce",
			Filter: ListFilter{Keyspace: []string{"keysapce.table2"}},
			Dir:    dir,
			Prune:  true,
		},
		{
			Name:   "filter min date",
			Filter: ListFilter{MinDate: timeutc.Now().Add(time.Hour)},
			Dir:    dir,
			Prune:  true,
		},
		{
			Name:   "filter max date",
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

	client, cl := scyllaclienttest.NewFakeRcloneServer(t, scyllaclienttest.PathFileMatcher("/metrics", "testdata/walker/scylla_metrics/metrics"))
	defer cl()

	manifests, err := listManifests(context.Background(), client, scyllaclienttest.TestHost, Location{Provider: "listtest"}, ListFilter{}, log.NewDevelopment())
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

	if diff := cmp.Diff(manifests, golden, UUIDComparer()); diff != "" {
		t.Fatalf("listManifests() = %v, diff %s", manifests, diff)
	}
}
