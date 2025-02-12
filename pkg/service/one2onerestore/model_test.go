// Copyright (C) 2025 ScyllaDB

package one2onerestore

import "testing"

func TestValidateNodesMapping(t *testing.T) {
	testCases := []struct {
		name         string
		nodesMapping []nodeMapping
		expectedErr  bool
	}{
		{
			name: "Everything is fine",
			nodesMapping: []nodeMapping{
				{Source: node{DC: "dc1", Rack: "rack1", HostID: "h1"}, Target: node{DC: "dc2", Rack: "rack2", HostID: "host1"}},
				{Source: node{DC: "dc1", Rack: "rack1", HostID: "h2"}, Target: node{DC: "dc2", Rack: "rack2", HostID: "host2"}},
			},
			expectedErr: false,
		},
		{
			name:         "Empty",
			nodesMapping: []nodeMapping{},
			expectedErr:  true,
		},
		{
			name: "DC count mismatch, 2 in source and 1 in target",
			nodesMapping: []nodeMapping{
				{Source: node{DC: "dc1", Rack: "rack1", HostID: "h1"}, Target: node{DC: "dc2", Rack: "rack2", HostID: "host1"}},
				{Source: node{DC: "dc2", Rack: "rack1", HostID: "h2"}, Target: node{DC: "dc2", Rack: "rack2", HostID: "host2"}},
			},
			expectedErr: true,
		},
		{
			name: "DC count mismatch, 1 in source and 2 in target",
			nodesMapping: []nodeMapping{
				{Source: node{DC: "dc1", Rack: "rack1", HostID: "h1"}, Target: node{DC: "dc2", Rack: "rack2", HostID: "host1"}},
				{Source: node{DC: "dc1", Rack: "rack1", HostID: "h2"}, Target: node{DC: "dc3", Rack: "rack2", HostID: "host2"}},
			},
			expectedErr: true,
		},
		{
			name: "Rack count mismatch, 2 in source and 1 in target",
			nodesMapping: []nodeMapping{
				{Source: node{DC: "dc1", Rack: "rack1", HostID: "h1"}, Target: node{DC: "dc2", Rack: "rack2", HostID: "host1"}},
				{Source: node{DC: "dc1", Rack: "rack2", HostID: "h2"}, Target: node{DC: "dc2", Rack: "rack2", HostID: "host2"}},
			},
			expectedErr: true,
		},
		{
			name: "Rack count mismatch, 1 in source and 2 in target",
			nodesMapping: []nodeMapping{
				{Source: node{DC: "dc1", Rack: "rack1", HostID: "h1"}, Target: node{DC: "dc2", Rack: "rack1", HostID: "host1"}},
				{Source: node{DC: "dc1", Rack: "rack1", HostID: "h2"}, Target: node{DC: "dc2", Rack: "rack2", HostID: "host2"}},
			},
			expectedErr: true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			err := validateNodesMapping(tc.nodesMapping)
			if err != nil && !tc.expectedErr {
				t.Fatalf("Unexpected err: %v", err)
			}
			if err == nil && tc.expectedErr {
				t.Fatalf("Expected err, but got nil")
			}
		})
	}
}
