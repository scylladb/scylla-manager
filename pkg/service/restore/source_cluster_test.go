// Copyright (C) 2026 ScyllaDB

package restore

import (
	"encoding/json"
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/scylladb/scylla-manager/backupspec"
	"github.com/scylladb/scylla-manager/v3/pkg/util/uuid"
)

func TestFilterManifestsBySourceCluster(t *testing.T) {
	t.Parallel()

	a, b := uuid.MustRandom(), uuid.MustRandom()
	manifests := []*backupspec.ManifestInfo{
		{ClusterID: a, NodeID: "a1"},
		{ClusterID: b, NodeID: "b1"},
		{ClusterID: a, NodeID: "a2"},
	}

	testCases := []struct {
		name      string
		source    uuid.UUID
		wantNodes []string
	}{
		{name: "unset keeps every cluster", source: uuid.Nil, wantNodes: []string{"a1", "b1", "a2"}},
		{name: "one cluster", source: a, wantNodes: []string{"a1", "a2"}},
		{name: "unknown cluster", source: uuid.MustRandom(), wantNodes: nil},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			var got []string
			for _, m := range filterManifestsBySourceCluster(manifests, tc.source) {
				got = append(got, m.NodeID)
			}
			if diff := cmp.Diff(tc.wantNodes, got); diff != "" {
				t.Fatalf("nodes mismatch (-want +got):\n%s", diff)
			}
		})
	}
}

func TestTargetSourceClusterIDJSON(t *testing.T) {
	t.Parallel()

	id := uuid.MustParse("b703df56-c428-46a7-bfba-cfa6ee91b976")
	testCases := []struct {
		name       string
		properties string
		want       uuid.UUID
	}{
		{name: "absent", properties: `{}`, want: uuid.Nil},
		{name: "set", properties: `{"source_cluster_id":"b703df56-c428-46a7-bfba-cfa6ee91b976"}`, want: id},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			var target Target
			if err := json.Unmarshal([]byte(tc.properties), &target); err != nil {
				t.Fatal(err)
			}
			if target.SourceClusterID != tc.want {
				t.Fatalf("SourceClusterID = %s, want %s", target.SourceClusterID, tc.want)
			}
			b, err := json.Marshal(target)
			if err != nil {
				t.Fatal(err)
			}
			var m map[string]any
			if err := json.Unmarshal(b, &m); err != nil {
				t.Fatal(err)
			}
			if _, ok := m["source_cluster_id"]; ok != (tc.want != uuid.Nil) {
				t.Fatalf("source_cluster_id present = %v in %s", ok, b)
			}
		})
	}
}
