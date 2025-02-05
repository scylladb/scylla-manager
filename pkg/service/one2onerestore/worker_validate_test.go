// Copyright (C) 2025 ScyllaDB

package one2onerestore

import (
	"strings"
	"testing"

	gocmp "github.com/google/go-cmp/cmp"
)

func TestMapTargetHostToSource(t *testing.T) {
	testCases := []struct {
		name string

		nodeMappings []nodeMapping
		targetHosts  []Host
		expected     map[string]Host
		expectedErr  bool
	}{
		{
			name: "All hosts have mappings",
			nodeMappings: []nodeMapping{
				{Source: node{DC: "dc1", Rack: "rack1", HostID: "host1"}, Target: node{DC: "dc4", Rack: "rack4", HostID: "host4"}},
				{Source: node{DC: "dc2", Rack: "rack2", HostID: "host2"}, Target: node{DC: "dc5", Rack: "rack5", HostID: "host5"}},
				{Source: node{DC: "dc3", Rack: "rack3", HostID: "host3"}, Target: node{DC: "dc6", Rack: "rack6", HostID: "host6"}},
			},
			targetHosts: []Host{
				{DC: "dc4", ID: "host4"},
				{DC: "dc5", ID: "host5"},
				{DC: "dc6", ID: "host6"},
			},
			expected: map[string]Host{
				"host1": {DC: "dc4", ID: "host4"},
				"host2": {DC: "dc5", ID: "host5"},
				"host3": {DC: "dc6", ID: "host6"},
			},
			expectedErr: false,
		},
		{
			name: "Mapping for target host is not found",
			nodeMappings: []nodeMapping{
				{Source: node{DC: "dc1", Rack: "rack1", HostID: "host1"}, Target: node{DC: "dc4", Rack: "rack4", HostID: "host4"}},
				{Source: node{DC: "dc2", Rack: "rack2", HostID: "host2"}, Target: node{DC: "dc5", Rack: "rack5", HostID: "host5"}},
				{Source: node{DC: "dc3", Rack: "rack3", HostID: "host3"}, Target: node{DC: "dc7", Rack: "rack7", HostID: "host7"}},
			},
			targetHosts: []Host{
				{DC: "dc4", ID: "host4"},
				{DC: "dc5", ID: "host5"},
				{DC: "dc6", ID: "host6"},
			},
			expected:    nil,
			expectedErr: true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			actual, err := mapTargetHostToSource(tc.targetHosts, tc.nodeMappings)
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

func TestMapSourceNodeToTarget(t *testing.T) {
	testCases := []struct {
		name string

		nodeMappings   []nodeMapping
		sourceNodeInfo []nodeValidationInfo
		expected       map[node]nodeValidationInfo
		expectedErr    bool
	}{
		{
			name: "All nodes have mappings",
			nodeMappings: []nodeMapping{
				{Source: node{DC: "dc1", Rack: "rack1", HostID: "host1"}, Target: node{DC: "dc4", Rack: "rack4", HostID: "host4"}},
				{Source: node{DC: "dc2", Rack: "rack2", HostID: "host2"}, Target: node{DC: "dc5", Rack: "rack5", HostID: "host5"}},
				{Source: node{DC: "dc3", Rack: "rack3", HostID: "host3"}, Target: node{DC: "dc6", Rack: "rack6", HostID: "host6"}},
			},
			sourceNodeInfo: []nodeValidationInfo{
				{DC: "dc1", Rack: "rack1", HostID: "host1"},
				{DC: "dc2", Rack: "rack2", HostID: "host2"},
				{DC: "dc3", Rack: "rack3", HostID: "host3"},
			},
			expected: map[node]nodeValidationInfo{
				{DC: "dc4", Rack: "rack4", HostID: "host4"}: {DC: "dc1", Rack: "rack1", HostID: "host1"},
				{DC: "dc5", Rack: "rack5", HostID: "host5"}: {DC: "dc2", Rack: "rack2", HostID: "host2"},
				{DC: "dc6", Rack: "rack6", HostID: "host6"}: {DC: "dc3", Rack: "rack3", HostID: "host3"},
			},
			expectedErr: false,
		},
		{
			name: "Mapping for source node is not found",
			nodeMappings: []nodeMapping{
				{Source: node{DC: "dc1", Rack: "rack1", HostID: "host1"}, Target: node{DC: "dc4", Rack: "rack4", HostID: "host4"}},
				{Source: node{DC: "dc2", Rack: "rack2", HostID: "host2"}, Target: node{DC: "dc5", Rack: "rack5", HostID: "host5"}},
				{Source: node{DC: "dc3", Rack: "rack3", HostID: "host3"}, Target: node{DC: "dc7", Rack: "rack7", HostID: "host7"}},
			},
			sourceNodeInfo: []nodeValidationInfo{
				{DC: "dc1", Rack: "rack1", HostID: "host1"},
				{DC: "dc2", Rack: "rack2", HostID: "host2"},
				{DC: "hi", Rack: "rack3", HostID: "host3"},
			},
			expected:    nil,
			expectedErr: true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			actual, err := mapSourceNodesToTarget(tc.sourceNodeInfo, tc.nodeMappings)
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

func TestCheckOne2OneRestoreCompatibility(t *testing.T) {
	testCases := []struct {
		name string

		sourceCluster, targetCluster []nodeValidationInfo
		nodeMappings                 []nodeMapping
		expectedErr                  string
	}{
		{
			name: "Compatible clusters",
			sourceCluster: []nodeValidationInfo{
				{DC: "dc1", Rack: "rack1", HostID: "host1", ShardCount: 4, StorageSize: 100, Tokens: []int64{1, 2, 3}},
				{DC: "dc2", Rack: "rack2", HostID: "host2", ShardCount: 8, StorageSize: 200, Tokens: []int64{4, 5, 6}},
			},
			targetCluster: []nodeValidationInfo{
				{DC: "dc4", Rack: "rack4", HostID: "host4", ShardCount: 4, StorageSize: 100, Tokens: []int64{1, 2, 3}},
				{DC: "dc5", Rack: "rack5", HostID: "host5", ShardCount: 8, StorageSize: 200, Tokens: []int64{4, 5, 6}},
			},
			nodeMappings: []nodeMapping{
				{Source: node{DC: "dc1", Rack: "rack1", HostID: "host1"}, Target: node{DC: "dc4", Rack: "rack4", HostID: "host4"}},
				{Source: node{DC: "dc2", Rack: "rack2", HostID: "host2"}, Target: node{DC: "dc5", Rack: "rack5", HostID: "host5"}},
			},
			expectedErr: "",
		},
		{
			name: "Different nodes count",
			sourceCluster: []nodeValidationInfo{
				{DC: "dc1", Rack: "rack1", HostID: "host1", ShardCount: 4, StorageSize: 100, Tokens: []int64{1, 2, 3}},
			},
			targetCluster: []nodeValidationInfo{
				{DC: "dc4", Rack: "rack4", HostID: "host4", ShardCount: 4, StorageSize: 100, Tokens: []int64{1, 2, 3}},
				{DC: "dc5", Rack: "rack5", HostID: "host5", ShardCount: 8, StorageSize: 200, Tokens: []int64{4, 5, 6}},
			},
			nodeMappings: []nodeMapping{
				{Source: node{DC: "dc1", Rack: "rack1", HostID: "host1"}, Target: node{DC: "dc4", Rack: "rack4", HostID: "host4"}},
				{Source: node{DC: "dc2", Rack: "rack2", HostID: "host2"}, Target: node{DC: "dc5", Rack: "rack5", HostID: "host5"}},
			},
			expectedErr: "clusters have different nodes count: source 1 != target 2",
		},
		{
			name: "Different ShardCount",
			sourceCluster: []nodeValidationInfo{
				{DC: "dc1", Rack: "rack1", HostID: "host1", ShardCount: 4, StorageSize: 100, Tokens: []int64{1, 2, 3}},
				{DC: "dc2", Rack: "rack2", HostID: "host2", ShardCount: 8, StorageSize: 200, Tokens: []int64{4, 5, 6}},
			},
			targetCluster: []nodeValidationInfo{
				{DC: "dc4", Rack: "rack4", HostID: "host4", ShardCount: 8, StorageSize: 100, Tokens: []int64{1, 2, 3}},
				{DC: "dc5", Rack: "rack5", HostID: "host5", ShardCount: 8, StorageSize: 200, Tokens: []int64{4, 5, 6}},
			},
			nodeMappings: []nodeMapping{
				{Source: node{DC: "dc1", Rack: "rack1", HostID: "host1"}, Target: node{DC: "dc4", Rack: "rack4", HostID: "host4"}},
				{Source: node{DC: "dc2", Rack: "rack2", HostID: "host2"}, Target: node{DC: "dc5", Rack: "rack5", HostID: "host5"}},
			},
			expectedErr: "source ShardCount doesn't match target ShardCount",
		},
		{
			name: "StorageSize greater in source",
			sourceCluster: []nodeValidationInfo{
				{DC: "dc1", Rack: "rack1", HostID: "host1", ShardCount: 4, StorageSize: 200, Tokens: []int64{1, 2, 3}},
				{DC: "dc2", Rack: "rack2", HostID: "host2", ShardCount: 8, StorageSize: 200, Tokens: []int64{4, 5, 6}},
			},
			targetCluster: []nodeValidationInfo{
				{DC: "dc4", Rack: "rack4", HostID: "host4", ShardCount: 4, StorageSize: 100, Tokens: []int64{1, 2, 3}},
				{DC: "dc5", Rack: "rack5", HostID: "host5", ShardCount: 8, StorageSize: 200, Tokens: []int64{4, 5, 6}},
			},
			nodeMappings: []nodeMapping{
				{Source: node{DC: "dc1", Rack: "rack1", HostID: "host1"}, Target: node{DC: "dc4", Rack: "rack4", HostID: "host4"}},
				{Source: node{DC: "dc2", Rack: "rack2", HostID: "host2"}, Target: node{DC: "dc5", Rack: "rack5", HostID: "host5"}},
			},
			expectedErr: "source StorageSize greater than target StorageSize",
		},
		{
			name: "Different Tokens",
			sourceCluster: []nodeValidationInfo{
				{DC: "dc1", Rack: "rack1", HostID: "host1", ShardCount: 4, StorageSize: 100, Tokens: []int64{1, 2, 3}},
				{DC: "dc2", Rack: "rack2", HostID: "host2", ShardCount: 8, StorageSize: 200, Tokens: []int64{4, 5, 6}},
			},
			targetCluster: []nodeValidationInfo{
				{DC: "dc4", Rack: "rack4", HostID: "host4", ShardCount: 4, StorageSize: 100, Tokens: []int64{4, 5, 6}},
				{DC: "dc5", Rack: "rack5", HostID: "host5", ShardCount: 8, StorageSize: 200, Tokens: []int64{7, 8, 9}},
			},
			nodeMappings: []nodeMapping{
				{Source: node{DC: "dc1", Rack: "rack1", HostID: "host1"}, Target: node{DC: "dc4", Rack: "rack4", HostID: "host4"}},
				{Source: node{DC: "dc2", Rack: "rack2", HostID: "host2"}, Target: node{DC: "dc5", Rack: "rack5", HostID: "host5"}},
			},
			expectedErr: "source Tokens doesn't match target Tokens",
		},
		{
			name: "Wrong HostID in node mappings",
			sourceCluster: []nodeValidationInfo{
				{DC: "dc1", Rack: "rack1", HostID: "host1", ShardCount: 4, StorageSize: 100, Tokens: []int64{1, 2, 3}},
				{DC: "dc2", Rack: "rack2", HostID: "host2", ShardCount: 8, StorageSize: 200, Tokens: []int64{4, 5, 6}},
			},
			targetCluster: []nodeValidationInfo{
				{DC: "dc4", Rack: "rack4", HostID: "host4", ShardCount: 4, StorageSize: 100, Tokens: []int64{1, 2, 3}},
				{DC: "dc5", Rack: "rack5", HostID: "host5", ShardCount: 8, StorageSize: 200, Tokens: []int64{4, 5, 6}},
			},
			nodeMappings: []nodeMapping{
				{Source: node{DC: "dc1", Rack: "rack1", HostID: "host1"}, Target: node{DC: "dc4", Rack: "rack4", HostID: "hello"}},
				{Source: node{DC: "dc2", Rack: "rack2", HostID: "host2"}, Target: node{DC: "dc5", Rack: "rack5", HostID: "world"}},
			},
			expectedErr: "target node has no match in source cluster",
		},
		{
			name: "Wrong Rack in node mappings",
			sourceCluster: []nodeValidationInfo{
				{DC: "dc1", Rack: "rack1", HostID: "host1", ShardCount: 4, StorageSize: 100, Tokens: []int64{1, 2, 3}},
				{DC: "dc2", Rack: "rack2", HostID: "host2", ShardCount: 8, StorageSize: 200, Tokens: []int64{4, 5, 6}},
			},
			targetCluster: []nodeValidationInfo{
				{DC: "dc4", Rack: "rack4", HostID: "host4", ShardCount: 4, StorageSize: 100, Tokens: []int64{1, 2, 3}},
				{DC: "dc5", Rack: "rack5", HostID: "host5", ShardCount: 8, StorageSize: 200, Tokens: []int64{4, 5, 6}},
			},
			nodeMappings: []nodeMapping{
				{Source: node{DC: "dc1", Rack: "rack1", HostID: "host1"}, Target: node{DC: "dc4", Rack: "hello", HostID: "host4"}},
				{Source: node{DC: "dc2", Rack: "rack2", HostID: "host2"}, Target: node{DC: "dc5", Rack: "rack5", HostID: "host5"}},
			},
			expectedErr: "target node has no match in source cluster",
		},
		{
			name: "Wrong DC in node mappings",
			sourceCluster: []nodeValidationInfo{
				{DC: "dc1", Rack: "rack1", HostID: "host1", ShardCount: 4, StorageSize: 100, Tokens: []int64{1, 2, 3}},
				{DC: "dc2", Rack: "rack2", HostID: "host2", ShardCount: 8, StorageSize: 200, Tokens: []int64{4, 5, 6}},
			},
			targetCluster: []nodeValidationInfo{
				{DC: "dc4", Rack: "rack4", HostID: "host4", ShardCount: 4, StorageSize: 100, Tokens: []int64{1, 2, 3}},
				{DC: "dc5", Rack: "rack5", HostID: "host5", ShardCount: 8, StorageSize: 200, Tokens: []int64{4, 5, 6}},
			},
			nodeMappings: []nodeMapping{
				{Source: node{DC: "dc1", Rack: "rack1", HostID: "host1"}, Target: node{DC: "dc4", Rack: "rack4", HostID: "host4"}},
				{Source: node{DC: "dc2", Rack: "rack2", HostID: "host2"}, Target: node{DC: "marvel", Rack: "rack5", HostID: "host5"}},
			},
			expectedErr: "target node has no match in source cluster:",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			actual := checkOne2OneRestoreCompatibility(tc.sourceCluster, tc.targetCluster, tc.nodeMappings)
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
