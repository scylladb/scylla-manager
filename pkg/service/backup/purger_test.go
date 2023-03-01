// Copyright (C) 2017 ScyllaDB

package backup

import (
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	. "github.com/scylladb/scylla-manager/v3/pkg/service/backup/backupspec"
	"github.com/scylladb/scylla-manager/v3/pkg/util/timeutc"
	"github.com/scylladb/scylla-manager/v3/pkg/util/uuid"
)

func TestStaleTags(t *testing.T) {
	gen := func(nodeID string, taskID uuid.UUID, a, b int) (manifests []*ManifestInfo) {
		for i := a; i < b; i++ {
			manifests = append(manifests, &ManifestInfo{
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
		task3     = uuid.MustRandom()
		task4     = uuid.MustRandom()
		manifests []*ManifestInfo
	)
	// Mixed snapshot tags across nodes
	manifests = append(manifests, gen("a", task0, 0, 7)...)
	manifests = append(manifests, gen("b", task0, 3, 5)...)
	// Valid nothing to do
	manifests = append(manifests, gen("a", task1, 10, 12)...)
	manifests = append(manifests, gen("b", task1, 10, 12)...)
	// Not found in policy delete older than 30 days
	manifests = append(manifests, gen("c", task2, 20, 22)...)
	manifests = append(manifests, &ManifestInfo{
		NodeID:      "c",
		TaskID:      task2,
		SnapshotTag: SnapshotTagAt(timeutc.Now().AddDate(0, 0, -15)),
	})
	// Mixed policy 1 - retention days deletes 2, retention days deletes 1
	manifests = append(manifests, gen("c", task3, 30, 32)...)
	manifests = append(manifests, &ManifestInfo{
		NodeID:      "c",
		TaskID:      task3,
		SnapshotTag: SnapshotTagAt(timeutc.Now().AddDate(0, 0, -7)),
	})
	// Mixed policy 2 - retention days deletes 1, retention days deletes 2
	deletedByRetentionTag := SnapshotTagAt(timeutc.Now().AddDate(0, 0, -7))
	manifests = append(manifests, gen("c", task4, 40, 41)...)
	manifests = append(manifests, &ManifestInfo{
		NodeID:      "c",
		TaskID:      task4,
		SnapshotTag: deletedByRetentionTag,
	})
	manifests = append(manifests, &ManifestInfo{
		NodeID:      "c",
		TaskID:      task4,
		SnapshotTag: SnapshotTagAt(timeutc.Now().AddDate(0, 0, -3)),
	})
	// Temporary manifest
	x := gen("c", task0, 6, 7)[0]
	x.Temporary = true
	manifests = append(manifests, x)

	tags, oldest, err := staleTags(manifests, RetentionMap{
		task0: {Retention: 3, RetentionDays: 0},
		task1: {Retention: 2, RetentionDays: 0},
		task3: {Retention: 2, RetentionDays: 10},
		task4: {Retention: 1, RetentionDays: 10},
	})
	if err != nil {
		t.Fatal(err)
	}
	if !oldest.Equal(time.Unix(4, 0)) {
		t.Fatal("Validate the time of the oldest, remaining backup")
	}

	golden := []string{
		"sm_19700101000000UTC",
		"sm_19700101000001UTC",
		"sm_19700101000002UTC",
		"sm_19700101000003UTC",
		"sm_19700101000006UTC",
		"sm_19700101000020UTC",
		"sm_19700101000021UTC",
		"sm_19700101000030UTC",
		"sm_19700101000031UTC",
		"sm_19700101000040UTC",
		deletedByRetentionTag,
	}

	if diff := cmp.Diff(tags.List(), golden, cmpopts.SortSlices(func(a, b string) bool { return a < b })); diff != "" {
		t.Fatalf("staleTags() = %s, diff:\n%s", tags.List(), diff)
	}
}
