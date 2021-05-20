// Copyright (C) 2017 ScyllaDB

package backup

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	"github.com/scylladb/scylla-manager/pkg/scyllaclient/scyllaclienttest"
	. "github.com/scylladb/scylla-manager/pkg/service/backup/backupspec"
	"github.com/scylladb/scylla-manager/pkg/testutils"
	"github.com/scylladb/scylla-manager/pkg/util/uuid"
)

func TestListAllManifests(t *testing.T) {
	client, closeServer := scyllaclienttest.NewFakeRcloneServer(t)
	defer closeServer()

	ctx := context.Background()

	manifests, error := listAllManifests(ctx, client, scyllaclienttest.TestHost,
		Location{Provider: "testdata", Path: "list"},
		uuid.MustParse("2e4ac82f-a7b5-4b6d-ab5e-0a1553a50a21"),
	)
	if error != nil {
		t.Fatal("ListAllManifests() error", error)
	}
	testutils.SaveGoldenJSONFileIfNeeded(t, manifests)
	var golden []*RemoteManifest
	testutils.LoadGoldenJSONFile(t, &golden)
	if diff := cmp.Diff(manifests, golden, testutils.UUIDComparer(), cmpopts.SortSlices(func(a, b *RemoteManifest) bool {
		if v := strings.Compare(a.NodeID, b.NodeID); v != 0 {
			return v < 0
		}
		if v := strings.Compare(a.SnapshotTag, b.SnapshotTag); v != 0 {
			return v < 0
		}
		return false
	})); diff != "" {
		t.Fatalf("ListAllManifests() diff\n%s", diff)
	}
}

func TestStaleTags(t *testing.T) {
	gen := func(nodeID string, taskID uuid.UUID, a, b int) (manifests []*RemoteManifest) {
		for i := a; i < b; i++ {
			manifests = append(manifests, &RemoteManifest{
				NodeID:      nodeID,
				TaskID:      taskID,
				SnapshotTag: SnapshotTagAt(time.Unix(int64(i), 0)),
			})
		}
		return
	}

	var (
		task0     = uuid.MustRandom()
		task1     = uuid.MustRandom()
		task2     = uuid.MustRandom()
		manifests []*RemoteManifest
	)
	// Mixed snapshot tags across nodes
	manifests = append(manifests, gen("a", task0, 0, 7)...)
	manifests = append(manifests, gen("b", task0, 3, 5)...)
	// Valid nothing to do
	manifests = append(manifests, gen("a", task1, 10, 12)...)
	manifests = append(manifests, gen("b", task1, 10, 12)...)
	// Not found in policy delete all
	manifests = append(manifests, gen("c", task2, 20, 22)...)
	// Temporary manifest
	x := gen("c", task0, 6, 7)[0]
	x.Temporary = true
	manifests = append(manifests, x)

	tags := staleTags(manifests, map[uuid.UUID]int{task0: 3, task1: 2}, time.Unix(21, 0))

	golden := []string{
		"sm_19700101000000UTC",
		"sm_19700101000001UTC",
		"sm_19700101000002UTC",
		"sm_19700101000003UTC",
		"sm_19700101000006UTC",
		"sm_19700101000020UTC",
	}

	if diff := cmp.Diff(tags.List(), golden, cmpopts.SortSlices(func(a, b string) bool { return a < b })); diff != "" {
		t.Fatalf("staleTags() = %s, diff:\n%s", tags.List(), diff)
	}
}
