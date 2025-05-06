// Copyright (C) 2025 ScyllaDB

package one2onerestore

import (
	"testing"

	"github.com/scylladb/scylla-manager/v3/pkg/scyllaclient"
)

func TestFindNodeFromDC(t *testing.T) {
	testCases := []struct {
		name         string
		nodes        scyllaclient.NodeStatusInfoSlice
		locationDC   string
		nodeMappings []nodeMapping

		expectedNodeAddr string
		expectedErr      bool
	}{
		{
			name: "Everything is fine",
			nodes: scyllaclient.NodeStatusInfoSlice{
				{Datacenter: "dc2", Addr: "h2"},
			},
			locationDC: "dc1",
			nodeMappings: []nodeMapping{
				{Source: node{DC: "dc1", Rack: "rack1", HostID: "h1"}, Target: node{DC: "dc2", Rack: "rack2", HostID: "h2"}},
			},

			expectedNodeAddr: "h2",
			expectedErr:      false,
		},
		{
			name: "Empty location DC",
			nodes: scyllaclient.NodeStatusInfoSlice{
				{Datacenter: "dc2", Addr: "h2"},
			},
			locationDC: "",
			nodeMappings: []nodeMapping{
				{Source: node{DC: "dc1", Rack: "rack1", HostID: "h1"}, Target: node{DC: "dc2", Rack: "rack2", HostID: "h2"}},
			},

			expectedNodeAddr: "h2",
			expectedErr:      false,
		},
		{
			name: "Mapping for location DC is not found",
			nodes: scyllaclient.NodeStatusInfoSlice{
				{Datacenter: "dc2", Addr: "h2"},
			},
			locationDC: "dc1",
			nodeMappings: []nodeMapping{
				{Source: node{DC: "dc0", Rack: "rack0", HostID: "h0"}, Target: node{DC: "dc2", Rack: "rack2", HostID: "h2"}},
			},
			expectedErr: true,
		},
		{
			name: "No target node with access to location DC",
			nodes: scyllaclient.NodeStatusInfoSlice{
				{Datacenter: "dc3", Addr: "h3"},
			},
			locationDC: "dc1",
			nodeMappings: []nodeMapping{
				{Source: node{DC: "dc1", Rack: "rack1", HostID: "h1"}, Target: node{DC: "dc2", Rack: "rack2", HostID: "h2"}},
			},
			expectedErr: true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			nodeAddr, err := findNodeFromDC(tc.nodes, tc.locationDC, tc.nodeMappings)
			if err == nil && tc.expectedErr {
				t.Fatalf("Expected err, but got nil")
			}
			if err != nil && !tc.expectedErr {
				t.Fatalf("Unexpected err: %v", err)
			}
			if nodeAddr != tc.expectedNodeAddr {
				t.Fatalf("Expected node %q, but got %q", tc.expectedNodeAddr, nodeAddr)
			}
		})
	}
}

func TestTabletKeyspacesAreNotSupported(t *testing.T) {
	testCases := []struct {
		name            string
		keyspaceFilter  []string
		tabletKeyspaces []string
		expectedErr     string
	}{
		{
			name:            "no tablet keyspaces",
			keyspaceFilter:  []string{"*"},
			tabletKeyspaces: nil,
		},
		{
			name:            "tablet keyspace, but not included into the keyspaceFilter",
			keyspaceFilter:  []string{"vnode-keyspace"},
			tabletKeyspaces: []string{"tablet-keyspace"},
		},
		{
			name:            "tablet keyspace, included into the keyspacesFilter",
			keyspaceFilter:  []string{"*"},
			tabletKeyspaces: []string{"tablet-keyspace"},
			expectedErr:     "1-1-restore doesn't support tablet based replication. Keyspace: tablet-keyspace",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			err := tabletKeyspacesAreNotSupported(tc.keyspaceFilter, tc.tabletKeyspaces)
			if err != nil {
				if tc.expectedErr != err.Error() {
					t.Fatalf("Expected err: %s, but got %s", tc.expectedErr, err.Error())
				}
			}
			if err == nil && tc.expectedErr != "" {
				t.Fatalf("Expected err: %s, but got nil", tc.expectedErr)
			}
		})
	}
}
