// Copyright (C) 2017 ScyllaDB

package backup

import (
	"context"
	"strings"
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	"github.com/scylladb/scylla-manager/pkg/scyllaclient/scyllaclienttest"
	. "github.com/scylladb/scylla-manager/pkg/service/backup/backupspec"
	"github.com/scylladb/scylla-manager/pkg/testutils"
	"github.com/scylladb/scylla-manager/pkg/util/uuid"
)

// TODO add test with support for listing files for all clusters with clusterID=Nil

func TestListManifests(t *testing.T) {
	client, closeServer := scyllaclienttest.NewFakeRcloneServer(t)
	defer closeServer()

	ctx := context.Background()

	t.Run("one cluster", func(t *testing.T) {
		manifests, error := listManifests(ctx, client, scyllaclienttest.TestHost,
			Location{Provider: "testdata", Path: "list"},
			uuid.MustParse("2e4ac82f-a7b5-4b6d-ab5e-0a1553a50a21"),
		)
		if error != nil {
			t.Fatal("listManifests() error", error)
		}
		testutils.SaveGoldenJSONFileIfNeeded(t, manifests)
		var golden []*ManifestInfo
		testutils.LoadGoldenJSONFile(t, &golden)
		if diff := cmp.Diff(manifests, golden, testutils.UUIDComparer(), cmpopts.SortSlices(func(a, b *ManifestInfo) bool {
			if v := strings.Compare(a.NodeID, b.NodeID); v != 0 {
				return v < 0
			}
			if v := strings.Compare(a.SnapshotTag, b.SnapshotTag); v != 0 {
				return v < 0
			}
			return false
		})); diff != "" {
			t.Fatalf("listManifests() diff\n%s", diff)
		}
	})
}
