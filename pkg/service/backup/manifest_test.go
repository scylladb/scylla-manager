// Copyright (C) 2017 ScyllaDB

package backup

import (
	"bytes"
	"context"
	"encoding/json"
	"io/ioutil"
	"math/rand"
	"path"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	"github.com/scylladb/go-log"
	"github.com/scylladb/scylla-manager/pkg/scyllaclient/scyllaclienttest"
	. "github.com/scylladb/scylla-manager/pkg/service/backup/backupspec"
	. "github.com/scylladb/scylla-manager/pkg/testutils"
	"github.com/scylladb/scylla-manager/pkg/util/timeutc"
	"github.com/scylladb/scylla-manager/pkg/util/uuid"
)

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
	s0 := SnapshotTagAt(now.Add(1 * time.Hour))
	s1 := SnapshotTagAt(now.Add(2 * time.Hour))
	s3 := SnapshotTagAt(now.Add(3 * time.Hour))

	ks0 := "keyspace0"
	ks1 := "keyspace1"

	tb0 := "table0"
	tb1 := "table1"

	manifestSize := int64(1024)
	nodeCount := int64(2)

	var input []*RemoteManifest

	// Add product of all the possibilities 2^5 items
	for _, c := range []uuid.UUID{c0, c1} {
		for _, n := range []string{n0, n1} {
			for _, s := range []string{s0, s1} {
				var idx []FilesMeta
				for _, ks := range []string{ks0, ks1} {
					for _, tb := range []string{tb0, tb1} {
						idx = append(idx, FilesMeta{
							Keyspace: ks,
							Table:    tb,
						})
					}
				}

				m := &RemoteManifest{
					ClusterID:   c,
					NodeID:      n,
					SnapshotTag: s,
					Content: ManifestContent{
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
	input = append(input, &RemoteManifest{
		ClusterID:   c0,
		SnapshotTag: s3,
		Content: ManifestContent{
			Version: "v2",
			Index: []FilesMeta{
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
			Name:       "List overlapping snapshots",
			Location:   Location{Provider: "walker", Path: "overlap-snapshots"},
			GoldenFile: "testdata/walker/overlap-snapshots/golden.json",
			Filter: ListFilter{
				ClusterID:   uuid.MustParse("45e7257a-fe1d-439b-9759-918f34abf83c"),
				SnapshotTag: "sm_20200128120927UTC",
			},
		},
		{
			Name:       "List temporary manifests",
			Location:   Location{Provider: "walker", Path: "temporary"},
			GoldenFile: "testdata/walker/temporary/with.golden.json",
			Filter: ListFilter{
				Temporary: true,
			},
		},
		{
			Name:       "Don't list temporary manifests",
			Location:   Location{Provider: "walker", Path: "temporary"},
			GoldenFile: "testdata/walker/temporary/without.golden.json",
			Filter: ListFilter{
				Temporary: false,
			},
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

			var manifestPaths []string
			for _, m := range manifests {
				manifestPaths = append(manifestPaths, path.Join(m.CleanPath...))
			}

			if UpdateGoldenFiles() {
				b, err := json.Marshal(manifestPaths)
				if err != nil {
					t.Fatal(err)
				}
				var buf bytes.Buffer
				if err := json.Indent(&buf, b, "", "  "); err != nil {
					t.Fatal(err)
				}
				if err := ioutil.WriteFile(test.GoldenFile, buf.Bytes(), 0666); err != nil {
					t.Error(err)
				}
			}

			b, err := ioutil.ReadFile(test.GoldenFile)
			if err != nil {
				t.Fatal(err)
			}
			var golden []string
			if err := json.Unmarshal(b, &golden); err != nil {
				t.Fatal(err)
			}

			if diff := cmp.Diff(golden, manifestPaths); diff != "" {
				t.Fatalf("listManifests() = %v, diff %s", manifests, diff)
			}
		})
	}
}
