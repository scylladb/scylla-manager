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

func TestCheckOne2OneRestoreCompatibility(t *testing.T) {
	testCases := []struct {
		name string

		sourceCluster, targetCluster nodeValidationInfo
		expectedErr                  string
	}{
		{
			name: "Compatible nodes",
			sourceCluster: nodeValidationInfo{
				DC: "dc1", Rack: "rack1", HostID: "host1", ShardCount: 4, StorageSize: 100, Tokens: []int64{1, 2, 3},
			},
			targetCluster: nodeValidationInfo{
				DC: "dc5", Rack: "rack5", HostID: "host5", ShardCount: 4, StorageSize: 200, Tokens: []int64{1, 2, 3},
			},
			expectedErr: "",
		},
		{
			name: "Different ShardCount",
			sourceCluster: nodeValidationInfo{
				DC: "dc1", Rack: "rack1", HostID: "host1", ShardCount: 4, StorageSize: 100, Tokens: []int64{1, 2, 3},
			},
			targetCluster: nodeValidationInfo{
				DC: "dc4", Rack: "rack4", HostID: "host4", ShardCount: 8, StorageSize: 100, Tokens: []int64{1, 2, 3},
			},
			expectedErr: "source ShardCount doesn't match target ShardCount",
		},
		{
			name: "StorageSize greater in source",
			sourceCluster: nodeValidationInfo{
				DC: "dc1", Rack: "rack1", HostID: "host1", ShardCount: 4, StorageSize: 200, Tokens: []int64{1, 2, 3},
			},
			targetCluster: nodeValidationInfo{
				DC: "dc4", Rack: "rack4", HostID: "host4", ShardCount: 4, StorageSize: 100, Tokens: []int64{1, 2, 3},
			},
			expectedErr: "source StorageSize greater than target StorageSize",
		},
		{
			name: "Different Tokens",
			sourceCluster: nodeValidationInfo{
				DC: "dc1", Rack: "rack1", HostID: "host1", ShardCount: 4, StorageSize: 100, Tokens: []int64{1, 2, 3},
			},
			targetCluster: nodeValidationInfo{
				DC: "dc4", Rack: "rack4", HostID: "host4", ShardCount: 4, StorageSize: 100, Tokens: []int64{4, 5, 6},
			},
			expectedErr: "source Tokens doesn't match target Tokens",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			actual := checkOne2OneRestoreCompatibility(tc.sourceCluster, tc.targetCluster)
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

func TestCheckNodeMappings(t *testing.T) {
	testCases := []struct {
		name         string
		sourceNode   nodeValidationInfo
		targetNode   nodeValidationInfo
		nodeMappings []nodeMapping
		error        string
	}{
		{
			name:       "Everything is fine",
			sourceNode: nodeValidationInfo{DC: "dc1", Rack: "rack1", HostID: "h1"},
			targetNode: nodeValidationInfo{DC: "dc2", Rack: "rack2", HostID: "h2"},
			nodeMappings: []nodeMapping{
				{Source: node{DC: "dc1", Rack: "rack1", HostID: "h1"}, Target: node{DC: "dc2", Rack: "rack2", HostID: "h2"}},
			},
		},
		{
			name:       "Different source mapping DC",
			sourceNode: nodeValidationInfo{DC: "dc1", Rack: "rack1", HostID: "h1"},
			targetNode: nodeValidationInfo{DC: "dc2", Rack: "rack2", HostID: "h2"},
			nodeMappings: []nodeMapping{
				{Source: node{DC: "dc", Rack: "rack1", HostID: "h1"}, Target: node{DC: "dc2", Rack: "rack2", HostID: "h2"}},
			},
			error: "mapping for source node is not found: dc1 rack1 h1",
		},
		{
			name:       "Different source mapping Rack",
			sourceNode: nodeValidationInfo{DC: "dc1", Rack: "rack1", HostID: "h1"},
			targetNode: nodeValidationInfo{DC: "dc2", Rack: "rack2", HostID: "h2"},
			nodeMappings: []nodeMapping{
				{Source: node{DC: "dc1", Rack: "rack", HostID: "h1"}, Target: node{DC: "dc2", Rack: "rack2", HostID: "h2"}},
			},
			error: "mapping for source node is not found: dc1 rack1 h1",
		},
		{
			name:       "Different source mapping HostID",
			sourceNode: nodeValidationInfo{DC: "dc1", Rack: "rack1", HostID: "h1"},
			targetNode: nodeValidationInfo{DC: "dc2", Rack: "rack2", HostID: "h2"},
			nodeMappings: []nodeMapping{
				{Source: node{DC: "dc1", Rack: "rack1", HostID: "h"}, Target: node{DC: "dc2", Rack: "rack2", HostID: "h2"}},
			},
			error: "mapping for source node is not found: dc1 rack1 h1",
		},
		{
			name:       "Different target mapping DC",
			sourceNode: nodeValidationInfo{DC: "dc1", Rack: "rack1", HostID: "h1"},
			targetNode: nodeValidationInfo{DC: "dc2", Rack: "rack2", HostID: "h2"},
			nodeMappings: []nodeMapping{
				{Source: node{DC: "dc1", Rack: "rack1", HostID: "h1"}, Target: node{DC: "dc", Rack: "rack2", HostID: "h2"}},
			},
			error: "mapping for target node is not found: dc2 rack2 h2",
		},
		{
			name:       "Different target mapping Rack",
			sourceNode: nodeValidationInfo{DC: "dc1", Rack: "rack1", HostID: "h1"},
			targetNode: nodeValidationInfo{DC: "dc2", Rack: "rack2", HostID: "h2"},
			nodeMappings: []nodeMapping{
				{Source: node{DC: "dc1", Rack: "rack1", HostID: "h1"}, Target: node{DC: "dc2", Rack: "rack", HostID: "h2"}},
			},
			error: "mapping for target node is not found: dc2 rack2 h2",
		},
		{
			name:       "Different target mapping HostID",
			sourceNode: nodeValidationInfo{DC: "dc1", Rack: "rack1", HostID: "h1"},
			targetNode: nodeValidationInfo{DC: "dc2", Rack: "rack2", HostID: "h2"},
			nodeMappings: []nodeMapping{
				{Source: node{DC: "dc1", Rack: "rack1", HostID: "h1"}, Target: node{DC: "dc2", Rack: "rack2", HostID: "h"}},
			},
			error: "mapping for target node is not found: dc2 rack2 h2",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			err := checkNodeMappings(tc.sourceNode, tc.targetNode, tc.nodeMappings)
			if err == nil && tc.error != "" {
				t.Fatalf("Expected err %q, but got nil", tc.error)
			}

			if err != nil && tc.error != "" {
				if err.Error() != tc.error {
					t.Fatalf("Expected err %q, but got %q", tc.error, err.Error())
				}
			}
		})
	}
}
