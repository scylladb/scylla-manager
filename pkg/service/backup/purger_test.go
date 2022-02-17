// Copyright (C) 2017 ScyllaDB

package backup

import (
	"github.com/scylladb/scylla-manager/pkg/util/timeutc"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	. "github.com/scylladb/scylla-manager/pkg/service/backup/backupspec"
	"github.com/scylladb/scylla-manager/pkg/util/uuid"
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

	// Temporary manifest
	x := gen("c", task0, 6, 7)[0]
	x.Temporary = true
	manifests = append(manifests, x)

	tags := staleTags(manifests, RetentionMap{task0: {0, 3}, task1: {0, 2}})

	golden := []string{
		"sm_19700101000000UTC",
		"sm_19700101000001UTC",
		"sm_19700101000002UTC",
		"sm_19700101000003UTC",
		"sm_19700101000006UTC",
		"sm_19700101000020UTC",
		"sm_19700101000021UTC",
	}

	if diff := cmp.Diff(tags.List(), golden, cmpopts.SortSlices(func(a, b string) bool { return a < b })); diff != "" {
		t.Fatalf("staleTags() = %s, diff:\n%s", tags.List(), diff)
	}
}
