// Copyright (C) 2025 ScyllaDB

package one2onerestore

import (
	"slices"
	"strings"
	"testing"

	"cmp"

	gocmp "github.com/google/go-cmp/cmp"

	"github.com/scylladb/scylla-manager/v3/pkg/service/backup/backupspec"
)

func TestApplyNodeMapping(t *testing.T) {
	testCases := []struct {
		name string

		sourceNodeInfo []nodeInfo
		sourceMappings map[node]node
		expected       []nodeInfo
		expectedErr    bool
	}{
		{
			name: "All nodes have mappings",
			sourceNodeInfo: []nodeInfo{
				{DC: "dc1", Rack: "rack1", HostID: "host1"},
				{DC: "dc2", Rack: "rack2", HostID: "host2"},
				{DC: "dc3", Rack: "rack3", HostID: "host3"},
			},
			sourceMappings: map[node]node{
				{DC: "dc1", Rack: "rack1", HostID: "host1"}: {DC: "dc4", Rack: "rack4", HostID: "host4"},
				{DC: "dc2", Rack: "rack2", HostID: "host2"}: {DC: "dc5", Rack: "rack5", HostID: "host5"},
				{DC: "dc3", Rack: "rack3", HostID: "host3"}: {DC: "dc6", Rack: "rack6", HostID: "host6"},
			},
			expected: []nodeInfo{
				{DC: "dc4", Rack: "rack4", HostID: "host4"},
				{DC: "dc5", Rack: "rack5", HostID: "host5"},
				{DC: "dc6", Rack: "rack6", HostID: "host6"},
			},
			expectedErr: false,
		},
		{
			name: "Source nodes count != node count in mappings",
			sourceNodeInfo: []nodeInfo{
				{DC: "dc1", Rack: "rack1", HostID: "host1"},
				{DC: "dc2", Rack: "rack2", HostID: "host2"},
			},
			sourceMappings: map[node]node{
				{DC: "dc1", Rack: "rack1", HostID: "host1"}: {DC: "dc4", Rack: "rack4", HostID: "host4"},
			},
			expectedErr: true,
		},
		{
			name: "Mapping for source node is not found",
			sourceNodeInfo: []nodeInfo{
				{DC: "dc1", Rack: "rack1", HostID: "host1"},
				{DC: "dc2", Rack: "rack2", HostID: "host2"},
			},
			sourceMappings: map[node]node{
				{DC: "dc1", Rack: "rack1", HostID: "host1"}: {DC: "dc4", Rack: "rack4", HostID: "host4"},
				{DC: "dc3", Rack: "rack3", HostID: "host3"}: {DC: "dc5", Rack: "rack5", HostID: "host5"},
			},
			expectedErr: true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			actual, err := applyNodeMapping(tc.sourceNodeInfo, tc.sourceMappings)
			if tc.expectedErr && err == nil {
				t.Fatalf("Expected err, but got nil")
			}
			if !tc.expectedErr && err != nil {
				t.Fatalf("Unexpected err: %v", err)
			}
			if diff := gocmp.Diff(actual, tc.expected); diff != "" {
				t.Fatalf("actual != expected\n%s", diff)
			}
		})
	}
}

func TestCompareNodesInfo(t *testing.T) {
	testCases := []struct {
		name string

		a, b     nodeInfo
		expected int
	}{
		{
			name:     "All fields equal",
			a:        nodeInfo{DC: "dc1", Rack: "rack1", HostID: "host1", CPUCount: 4, StorageSize: 100, Tokens: []int64{1, 2}},
			b:        nodeInfo{DC: "dc1", Rack: "rack1", HostID: "host1", CPUCount: 4, StorageSize: 100, Tokens: []int64{1, 2}},
			expected: 0,
		},
		{
			name:     "DC different",
			a:        nodeInfo{DC: "dc1", Rack: "rack1", HostID: "host1", CPUCount: 4, StorageSize: 100, Tokens: []int64{1, 2}},
			b:        nodeInfo{DC: "dc2", Rack: "rack1", HostID: "host1", CPUCount: 4, StorageSize: 100, Tokens: []int64{1, 2}},
			expected: -1,
		},
		{
			name:     "Rack different",
			a:        nodeInfo{DC: "dc1", Rack: "rack1", HostID: "host1", CPUCount: 4, StorageSize: 100, Tokens: []int64{1, 2}},
			b:        nodeInfo{DC: "dc1", Rack: "rack2", HostID: "host1", CPUCount: 4, StorageSize: 100, Tokens: []int64{1, 2}},
			expected: -1,
		},
		{
			name:     "HostID different",
			a:        nodeInfo{DC: "dc1", Rack: "rack1", HostID: "host1", CPUCount: 4, StorageSize: 100, Tokens: []int64{1, 2}},
			b:        nodeInfo{DC: "dc1", Rack: "rack1", HostID: "host2", CPUCount: 4, StorageSize: 100, Tokens: []int64{1, 2}},
			expected: -1,
		},
		{
			name:     "CPUCount different",
			a:        nodeInfo{DC: "dc1", Rack: "rack1", HostID: "host1", CPUCount: 4, StorageSize: 100, Tokens: []int64{1, 2}},
			b:        nodeInfo{DC: "dc1", Rack: "rack1", HostID: "host1", CPUCount: 5, StorageSize: 100, Tokens: []int64{1, 2}},
			expected: -1,
		},
		{
			name:     "StorageSize different",
			a:        nodeInfo{DC: "dc1", Rack: "rack1", HostID: "host1", CPUCount: 4, StorageSize: 100, Tokens: []int64{1, 2}},
			b:        nodeInfo{DC: "dc1", Rack: "rack1", HostID: "host1", CPUCount: 4, StorageSize: 200, Tokens: []int64{1, 2}},
			expected: -1,
		},
		{
			name:     "Tokens different",
			a:        nodeInfo{DC: "dc1", Rack: "rack1", HostID: "host1", CPUCount: 4, StorageSize: 100, Tokens: []int64{1, 2}},
			b:        nodeInfo{DC: "dc1", Rack: "rack1", HostID: "host1", CPUCount: 4, StorageSize: 100, Tokens: []int64{2, 3}},
			expected: -1,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			actual := compareNodesInfo(tc.a, tc.b)
			if actual != tc.expected {
				t.Fatalf("Expected %v, but got %v", tc.expected, actual)
			}
		})
	}
}

func TestClustersAreEqual(t *testing.T) {
	testCases := []struct {
		name string

		sourceCluster, targetCluster []nodeInfo
		expectedErr                  string
	}{
		{
			name: "Equal clusters",
			sourceCluster: []nodeInfo{
				{DC: "dc1", Rack: "rack1", HostID: "host1", CPUCount: 4, StorageSize: 100, Tokens: []int64{1, 2, 3}},
				{DC: "dc2", Rack: "rack2", HostID: "host2", CPUCount: 8, StorageSize: 200, Tokens: []int64{4, 5, 6}},
			},
			targetCluster: []nodeInfo{
				{DC: "dc1", Rack: "rack1", HostID: "host1", CPUCount: 4, StorageSize: 100, Tokens: []int64{1, 2, 3}},
				{DC: "dc2", Rack: "rack2", HostID: "host2", CPUCount: 8, StorageSize: 200, Tokens: []int64{4, 5, 6}},
			},
			expectedErr: "",
		},
		{
			name: "Different nodes count",
			sourceCluster: []nodeInfo{
				{DC: "dc1", Rack: "rack1", HostID: "host1", CPUCount: 4, StorageSize: 100, Tokens: []int64{1, 2, 3}},
			},
			targetCluster: []nodeInfo{
				{DC: "dc1", Rack: "rack1", HostID: "host1", CPUCount: 4, StorageSize: 100, Tokens: []int64{1, 2, 3}},
				{DC: "dc2", Rack: "rack2", HostID: "host2", CPUCount: 8, StorageSize: 200, Tokens: []int64{4, 5, 6}},
			},
			expectedErr: "clusters have different nodes count: source 1 != target 2",
		},
		{
			name: "Different DC",
			sourceCluster: []nodeInfo{
				{DC: "dc1", Rack: "rack1", HostID: "host1", CPUCount: 4, StorageSize: 100, Tokens: []int64{1, 2, 3}},
				{DC: "dc2", Rack: "rack2", HostID: "host2", CPUCount: 8, StorageSize: 200, Tokens: []int64{4, 5, 6}},
			},
			targetCluster: []nodeInfo{
				{DC: "dc3", Rack: "rack1", HostID: "host1", CPUCount: 4, StorageSize: 100, Tokens: []int64{1, 2, 3}},
				{DC: "dc2", Rack: "rack2", HostID: "host2", CPUCount: 8, StorageSize: 200, Tokens: []int64{4, 5, 6}},
			},
			expectedErr: "source DC doesn't match target DC",
		},
		{
			name: "Different Rack",
			sourceCluster: []nodeInfo{
				{DC: "dc1", Rack: "rack1", HostID: "host1", CPUCount: 4, StorageSize: 100, Tokens: []int64{1, 2, 3}},
				{DC: "dc2", Rack: "rack2", HostID: "host2", CPUCount: 8, StorageSize: 200, Tokens: []int64{4, 5, 6}},
			},
			targetCluster: []nodeInfo{
				{DC: "dc1", Rack: "rack3", HostID: "host1", CPUCount: 4, StorageSize: 100, Tokens: []int64{1, 2, 3}},
				{DC: "dc2", Rack: "rack2", HostID: "host2", CPUCount: 8, StorageSize: 200, Tokens: []int64{4, 5, 6}},
			},
			expectedErr: "source Rack doesn't match target Rack",
		},
		{
			name: "Different HostID",
			sourceCluster: []nodeInfo{
				{DC: "dc1", Rack: "rack1", HostID: "host1", CPUCount: 4, StorageSize: 100, Tokens: []int64{1, 2, 3}},
				{DC: "dc2", Rack: "rack2", HostID: "host2", CPUCount: 8, StorageSize: 200, Tokens: []int64{4, 5, 6}},
			},
			targetCluster: []nodeInfo{
				{DC: "dc1", Rack: "rack1", HostID: "host3", CPUCount: 4, StorageSize: 100, Tokens: []int64{1, 2, 3}},
				{DC: "dc2", Rack: "rack2", HostID: "host2", CPUCount: 8, StorageSize: 200, Tokens: []int64{4, 5, 6}},
			},
			expectedErr: "source HostID doesn't match target HostID",
		},
		{
			name: "Different CPUCount",
			sourceCluster: []nodeInfo{
				{DC: "dc1", Rack: "rack1", HostID: "host1", CPUCount: 4, StorageSize: 100, Tokens: []int64{1, 2, 3}},
				{DC: "dc2", Rack: "rack2", HostID: "host2", CPUCount: 8, StorageSize: 200, Tokens: []int64{4, 5, 6}},
			},
			targetCluster: []nodeInfo{
				{DC: "dc1", Rack: "rack1", HostID: "host1", CPUCount: 2, StorageSize: 100, Tokens: []int64{1, 2, 3}},
				{DC: "dc2", Rack: "rack2", HostID: "host2", CPUCount: 8, StorageSize: 200, Tokens: []int64{4, 5, 6}},
			},
			expectedErr: "source CPUCount doesn't match target CPUCount",
		},
		{
			name: "StorageSize greater in source",
			sourceCluster: []nodeInfo{
				{DC: "dc1", Rack: "rack1", HostID: "host1", CPUCount: 4, StorageSize: 200, Tokens: []int64{1, 2, 3}},
				{DC: "dc2", Rack: "rack2", HostID: "host2", CPUCount: 8, StorageSize: 200, Tokens: []int64{4, 5, 6}},
			},
			targetCluster: []nodeInfo{
				{DC: "dc1", Rack: "rack1", HostID: "host1", CPUCount: 4, StorageSize: 100, Tokens: []int64{1, 2, 3}},
				{DC: "dc2", Rack: "rack2", HostID: "host2", CPUCount: 8, StorageSize: 200, Tokens: []int64{4, 5, 6}},
			},
			expectedErr: "source StorageSize greater than target StorageSize",
		},
		{
			name: "Different Tokens",
			sourceCluster: []nodeInfo{
				{DC: "dc1", Rack: "rack1", HostID: "host1", CPUCount: 4, StorageSize: 100, Tokens: []int64{1, 2, 3}},
				{DC: "dc2", Rack: "rack2", HostID: "host2", CPUCount: 8, StorageSize: 200, Tokens: []int64{4, 5, 6}},
			},
			targetCluster: []nodeInfo{
				{DC: "dc1", Rack: "rack1", HostID: "host1", CPUCount: 4, StorageSize: 100, Tokens: []int64{7, 8, 9}},
				{DC: "dc2", Rack: "rack2", HostID: "host2", CPUCount: 8, StorageSize: 200, Tokens: []int64{4, 5, 6}},
			},
			expectedErr: "source Tokens doesn't match target Tokens",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			actual := clustersAreEqual(tc.sourceCluster, tc.targetCluster)
			if tc.expectedErr != "" && actual == nil {
				t.Fatalf("Expected err %q, but got nil", tc.expectedErr)
			}
			if tc.expectedErr == "" && actual != nil {
				t.Fatalf("Unexpected err: %v", actual)
			}
			if tc.expectedErr != "" && actual != nil {
				if !strings.HasPrefix(actual.Error(), tc.expectedErr) {
					t.Fatalf("Expected err %q, but got %q", tc.expectedErr, actual.Error())
				}
			}
		})
	}
}

func TestAssignHostToManifest(t *testing.T) {
	testCases := []struct {
		name string

		locations []LocationInfo
		expected  []hostManifest
	}{
		{
			name: "Single location, single manifest, single host",
			locations: []LocationInfo{
				{
					Hosts: []Host{
						{ID: "host1"},
					},
					Manifest: []*backupspec.ManifestInfo{
						{NodeID: "manifest1"},
					},
				},
			},
			expected: []hostManifest{
				{Host: Host{ID: "host1"}, Manifest: &backupspec.ManifestInfo{NodeID: "manifest1"}},
			},
		},
		{
			name: "Single location, multiple manifests, single host",
			locations: []LocationInfo{
				{
					Hosts: []Host{
						{ID: "host1"},
					},
					Manifest: []*backupspec.ManifestInfo{
						{NodeID: "manifest1"},
						{NodeID: "manifest2"},
					},
				},
			},
			expected: []hostManifest{
				{Host: Host{ID: "host1"}, Manifest: &backupspec.ManifestInfo{NodeID: "manifest1"}},
				{Host: Host{ID: "host1"}, Manifest: &backupspec.ManifestInfo{NodeID: "manifest2"}},
			},
		},
		{
			name: "Multiple locations, multiple manifests, multiple hosts",
			locations: []LocationInfo{
				{
					Hosts: []Host{
						{ID: "host1"},
						{ID: "host2"},
					},
					Manifest: []*backupspec.ManifestInfo{
						{NodeID: "manifest1"},
						{NodeID: "manifest2"},
					},
				},
				{
					Hosts: []Host{
						{ID: "host3"},
						{ID: "host4"},
					},
					Manifest: []*backupspec.ManifestInfo{
						{NodeID: "manifest3"},
						{NodeID: "manifest4"},
					},
				},
			},
			expected: []hostManifest{
				{Host: Host{ID: "host1"}, Manifest: &backupspec.ManifestInfo{NodeID: "manifest1"}},
				{Host: Host{ID: "host2"}, Manifest: &backupspec.ManifestInfo{NodeID: "manifest2"}},
				{Host: Host{ID: "host3"}, Manifest: &backupspec.ManifestInfo{NodeID: "manifest3"}},
				{Host: Host{ID: "host4"}, Manifest: &backupspec.ManifestInfo{NodeID: "manifest4"}},
			},
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			actual := assignHostToManifest(tc.locations)
			assertHostManifests(t, tc.expected, actual)
		})
	}
}

func assertHostManifests(t *testing.T, expected, actual []hostManifest) {
	t.Helper()
	slices.SortFunc(actual, compareTestHostManifests)
	slices.SortFunc(expected, compareTestHostManifests)

	equal := slices.EqualFunc(expected, actual, func(a, b hostManifest) bool {
		return compareTestHostManifests(a, b) == 0
	})

	if !equal {
		t.Fatalf("Expected %v, but got %v", expected, actual)
	}
}

func compareTestHostManifests(a, b hostManifest) int {
	return cmp.Or(
		cmp.Compare(a.Host.ID, b.Host.ID),
		cmp.Compare(a.Manifest.NodeID, b.Manifest.NodeID),
	)
}
