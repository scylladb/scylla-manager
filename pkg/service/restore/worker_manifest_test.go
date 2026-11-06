// Copyright (C) 2026 ScyllaDB

package restore

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/scylladb/scylla-manager/backupspec"
	"github.com/scylladb/scylla-manager/v3/pkg/util/uuid"
)

func TestManifestMatches(t *testing.T) {
	t.Parallel()

	const (
		tag      = "sm_20260101000000UTC"
		otherTag = "sm_20260102000000UTC"
	)
	c1 := uuid.MustRandom()
	c2 := uuid.MustRandom()

	testCases := []struct {
		name        string
		manifest    backupspec.ManifestInfo
		clusterID   uuid.UUID
		expectMatch bool
	}{
		{
			name:        "no source cluster, matching tag",
			manifest:    backupspec.ManifestInfo{ClusterID: c1, SnapshotTag: tag},
			clusterID:   uuid.Nil,
			expectMatch: true,
		},
		{
			name:        "no source cluster, matching tag of another cluster",
			manifest:    backupspec.ManifestInfo{ClusterID: c2, SnapshotTag: tag},
			clusterID:   uuid.Nil,
			expectMatch: true,
		},
		{
			name:        "no source cluster, other tag",
			manifest:    backupspec.ManifestInfo{ClusterID: c1, SnapshotTag: otherTag},
			clusterID:   uuid.Nil,
			expectMatch: false,
		},
		{
			name:        "source cluster, matching cluster and tag",
			manifest:    backupspec.ManifestInfo{ClusterID: c1, SnapshotTag: tag},
			clusterID:   c1,
			expectMatch: true,
		},
		{
			name:        "source cluster, other cluster with matching tag",
			manifest:    backupspec.ManifestInfo{ClusterID: c2, SnapshotTag: tag},
			clusterID:   c1,
			expectMatch: false,
		},
		{
			name:        "source cluster, matching cluster with other tag",
			manifest:    backupspec.ManifestInfo{ClusterID: c1, SnapshotTag: otherTag},
			clusterID:   c1,
			expectMatch: false,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			if got := manifestMatches(&tc.manifest, tag, tc.clusterID); got != tc.expectMatch {
				t.Fatalf("manifestMatches() = %v, expected %v", got, tc.expectMatch)
			}
		})
	}
}

func TestParseTargetSourceClusterID(t *testing.T) {
	t.Parallel()

	c1 := uuid.MustRandom()

	testCases := []struct {
		name     string
		props    string
		expected uuid.UUID
		errMsg   string
	}{
		{
			name:     "unset",
			props:    `{"location": ["s3:bucket"], "snapshot_tag": "sm_20260101000000UTC", "restore_tables": true}`,
			expected: uuid.Nil,
		},
		{
			name:     "set",
			props:    `{"location": ["s3:bucket"], "snapshot_tag": "sm_20260101000000UTC", "restore_tables": true, "source_cluster_id": "` + c1.String() + `"}`,
			expected: c1,
		},
		{
			name:   "invalid",
			props:  `{"location": ["s3:bucket"], "snapshot_tag": "sm_20260101000000UTC", "restore_tables": true, "source_cluster_id": "not-a-uuid"}`,
			errMsg: "invalid",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			target, err := parseTarget(json.RawMessage(tc.props))
			if tc.errMsg != "" {
				if err == nil || !strings.Contains(strings.ToLower(err.Error()), tc.errMsg) {
					t.Fatalf("parseTarget() error = %v, expected error containing %q", err, tc.errMsg)
				}
				return
			}
			if err != nil {
				t.Fatalf("parseTarget() error = %v", err)
			}
			if target.SourceClusterID != tc.expected {
				t.Fatalf("SourceClusterID = %s, expected %s", target.SourceClusterID, tc.expected)
			}

			// Unset source cluster shouldn't be visible in marshaled task properties
			raw, err := json.Marshal(target)
			if err != nil {
				t.Fatal(err)
			}
			if has := strings.Contains(string(raw), "source_cluster_id"); has != (tc.expected != uuid.Nil) {
				t.Fatalf("marshaled target %s, source_cluster_id present = %v", raw, has)
			}
		})
	}
}
