// Copyright (C) 2017 ScyllaDB

package backup

import (
	"context"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/scylladb/scylla-manager/pkg/scyllaclient/scyllaclienttest"
	. "github.com/scylladb/scylla-manager/pkg/service/backup/backupspec"
	"github.com/scylladb/scylla-manager/pkg/util/timeutc"
	"github.com/scylladb/scylla-manager/pkg/util/uuid"
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
			Dirs:  []string{""},
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

	client, closeServer := scyllaclienttest.NewFakeRcloneServer(t)
	defer closeServer()

	w := walker{
		Host:     scyllaclienttest.TestHost,
		Location: Location{Provider: "walker", Path: "simple"},
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

	client, closeServer := scyllaclienttest.NewFakeRcloneServer(t)
	defer closeServer()

	w := walker{
		Host:     scyllaclienttest.TestHost,
		Location: Location{Provider: "walker", Path: "simple"},
		Client:   client,
		PruneDir: func(dir string) bool {
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

	var (
		snapshotTag = NewSnapshotTag()
		dir         = RemoteManifestFile(uuid.MustRandom(), uuid.MustRandom(), snapshotTag, "dc", "nodeID")
	)

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
			Name:   "filter snapshot tag",
			Filter: ListFilter{SnapshotTag: snapshotTag},
			Dir:    dir,
			Prune:  false,
		},
		{
			Name:   "filter snapshot tag",
			Filter: ListFilter{SnapshotTag: "tag"},
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

			f := makeListFilterPruneDirFunc(test.Filter)
			if f(test.Dir) != test.Prune {
				t.Errorf("ListFilterPruneFunc() got %v expected %v", f(test.Dir), test.Prune)
			}
		})
	}
}
