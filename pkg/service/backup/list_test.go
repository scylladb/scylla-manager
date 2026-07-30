// Copyright (C) 2017 ScyllaDB

package backup

import (
	"context"
	"strings"
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	"github.com/scylladb/scylla-manager/backupspec"
	"github.com/scylladb/scylla-manager/v3/pkg/scyllaclient/scyllaclienttest"
	"github.com/scylladb/scylla-manager/v3/pkg/testutils"

	"github.com/scylladb/scylla-manager/v3/pkg/util/uuid"
)

// TODO add test with support for listing files for all clusters with clusterID=Nil

func TestListManifests(t *testing.T) {
	client, closeServer := scyllaclienttest.NewFakeRcloneServer(t)
	defer closeServer()

	ctx := context.Background()

	t.Run("one cluster", func(t *testing.T) {
		remoteManifests, error := listRemoteManifests(ctx, client, scyllaclienttest.TestHost,
			backupspec.Location{Provider: "testdata", Path: "list"},
			uuid.MustParse("2e4ac82f-a7b5-4b6d-ab5e-0a1553a50a21"),
		)
		if error != nil {
			t.Fatal("listManifests() error", error)
		}
		manifests := manifestInfos(remoteManifests)
		testutils.SaveGoldenJSONFileIfNeeded(t, manifests)
		var golden []*backupspec.ManifestInfo
		testutils.LoadGoldenJSONFile(t, &golden)
		if diff := cmp.Diff(manifests, golden, testutils.UUIDComparer(), cmpopts.SortSlices(func(a, b *backupspec.ManifestInfo) bool {
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

func TestFilterShadowedTemporaryManifests(t *testing.T) {
	t.Parallel()

	const (
		nodeA        = "a"
		nodeB        = "b"
		snapshotTagA = "sm_19700101000000UTC"
		snapshotTagB = "sm_19700101000001UTC"
	)
	var (
		taskID        = uuid.MustRandom()
		clusterID     = uuid.MustRandom()
		location      = backupspec.Location{Provider: backupspec.S3, Path: "test"}
		otherLocation = backupspec.Location{Provider: backupspec.S3, Path: "other"}
	)
	manifest := func(l backupspec.Location, nodeID, snapshotTag string, temporary bool) *backupspec.ManifestInfo {
		return &backupspec.ManifestInfo{
			Location:    l,
			DC:          "dc1",
			ClusterID:   clusterID,
			NodeID:      nodeID,
			TaskID:      taskID,
			SnapshotTag: snapshotTag,
			Temporary:   temporary,
		}
	}

	var (
		nodeATagARegular                = manifest(location, nodeA, snapshotTagA, false)
		nodeATagATemporary              = manifest(location, nodeA, snapshotTagA, true)
		nodeBTagATemporary              = manifest(location, nodeB, snapshotTagA, true)
		nodeATagBTemporary              = manifest(location, nodeA, snapshotTagB, true)
		nodeATagATemporaryOtherLocation = manifest(otherLocation, nodeA, snapshotTagA, true)
		nodeBTagBRegular                = manifest(location, nodeB, snapshotTagB, false)
	)

	tests := []struct {
		name      string
		manifests []*backupspec.ManifestInfo
		expected  []*backupspec.ManifestInfo
	}{
		{
			name:      "shadowed temporary manifest is filtered out",
			manifests: []*backupspec.ManifestInfo{nodeATagARegular, nodeATagATemporary},
			expected:  []*backupspec.ManifestInfo{nodeATagARegular},
		},
		{
			name:      "temporary manifest without regular counterpart is kept",
			manifests: []*backupspec.ManifestInfo{nodeBTagBRegular, nodeATagATemporary},
			expected:  []*backupspec.ManifestInfo{nodeBTagBRegular, nodeATagATemporary},
		},
		{
			name:      "regular manifest of another node does not shadow temporary manifest",
			manifests: []*backupspec.ManifestInfo{nodeATagARegular, nodeBTagATemporary},
			expected:  []*backupspec.ManifestInfo{nodeATagARegular, nodeBTagATemporary},
		},
		{
			name:      "regular manifest of another snapshot tag does not shadow temporary manifest",
			manifests: []*backupspec.ManifestInfo{nodeATagARegular, nodeATagBTemporary},
			expected:  []*backupspec.ManifestInfo{nodeATagARegular, nodeATagBTemporary},
		},
		{
			name:      "regular manifest from another location does not shadow temporary manifest",
			manifests: []*backupspec.ManifestInfo{nodeATagARegular, nodeATagATemporaryOtherLocation},
			expected:  []*backupspec.ManifestInfo{nodeATagARegular, nodeATagATemporaryOtherLocation},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			if diff := cmp.Diff(filterShadowedTemporaryManifests(test.manifests), test.expected, testutils.UUIDComparer()); diff != "" {
				t.Fatalf("filterShadowedTemporaryManifests() diff\n%s", diff)
			}
		})
	}
}
