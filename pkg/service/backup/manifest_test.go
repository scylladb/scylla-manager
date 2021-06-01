// Copyright (C) 2017 ScyllaDB

package backup

import (
	"math/rand"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
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
