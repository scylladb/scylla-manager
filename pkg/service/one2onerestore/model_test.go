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
			name: "DC count mismatch, 2 in source and 1 in target, dc+racks are unique in source",
			nodesMapping: []nodeMapping{
				{Source: node{DC: "dc1", Rack: "rack1", HostID: "h1"}, Target: node{DC: "dc2", Rack: "rack2", HostID: "host1"}},
				{Source: node{DC: "dc2", Rack: "rack1", HostID: "h2"}, Target: node{DC: "dc2", Rack: "rack3", HostID: "host2"}},
			},
			expectedErr: true,
		},
		{
			name: "DC count mismatch, 1 in source and 2 in target, dc+racks are unique in target",
			nodesMapping: []nodeMapping{
				{Source: node{DC: "dc1", Rack: "rack1", HostID: "h1"}, Target: node{DC: "dc2", Rack: "rack2", HostID: "host1"}},
				{Source: node{DC: "dc1", Rack: "rack2", HostID: "h2"}, Target: node{DC: "dc3", Rack: "rack3", HostID: "host2"}},
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
		{
			name: "DCs are not mapped 1 to 1",
			nodesMapping: []nodeMapping{
				{Source: node{DC: "dc1", Rack: "rack1", HostID: "h1"}, Target: node{DC: "dc1", Rack: "rack1", HostID: "host1"}},
				{Source: node{DC: "dc1", Rack: "rack1", HostID: "h2"}, Target: node{DC: "dc2", Rack: "rack1", HostID: "host2"}},
				{Source: node{DC: "dc2", Rack: "rack1", HostID: "h3"}, Target: node{DC: "dc2", Rack: "rack1", HostID: "host3"}},
			},
			expectedErr: true,
		},
		{
			name: "Racks are not mapped 1 to 1",
			nodesMapping: []nodeMapping{
				{Source: node{DC: "dc1", Rack: "rack1", HostID: "h1"}, Target: node{DC: "dc2", Rack: "rack1", HostID: "host1"}},
				{Source: node{DC: "dc1", Rack: "rack2", HostID: "h2"}, Target: node{DC: "dc2", Rack: "rack2", HostID: "host3"}},
				{Source: node{DC: "dc1", Rack: "rack2", HostID: "h3"}, Target: node{DC: "dc2", Rack: "rack1", HostID: "host2"}},
			},
			expectedErr: true,
		},
		{
			name: "Nodes are not mapped 1 to 1, duplicate in the source",
			nodesMapping: []nodeMapping{
				{Source: node{DC: "dc1", Rack: "rack1", HostID: "h1"}, Target: node{DC: "dc2", Rack: "rack1", HostID: "host1"}},
				{Source: node{DC: "dc1", Rack: "rack1", HostID: "h1"}, Target: node{DC: "dc2", Rack: "rack1", HostID: "host3"}},
				{Source: node{DC: "dc1", Rack: "rack1", HostID: "h3"}, Target: node{DC: "dc2", Rack: "rack1", HostID: "host2"}},
			},
			expectedErr: true,
		},
		{
			name: "Nodes are not mapped 1 to 1, duplicate in the target",
			nodesMapping: []nodeMapping{
				{Source: node{DC: "dc1", Rack: "rack1", HostID: "h1"}, Target: node{DC: "dc2", Rack: "rack1", HostID: "host1"}},
				{Source: node{DC: "dc1", Rack: "rack1", HostID: "h2"}, Target: node{DC: "dc2", Rack: "rack1", HostID: "host3"}},
				{Source: node{DC: "dc1", Rack: "rack1", HostID: "h3"}, Target: node{DC: "dc2", Rack: "rack1", HostID: "host3"}},
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

func TestValidateKeyspaceFilter(t *testing.T) {
	testCases := []struct {
		name           string
		keyspaceFilter []string
		keyspaces      []string
		expectedErr    string
	}{
		{
			name:           "Only keyspace level filtering",
			keyspaceFilter: []string{"hello", "system"},
			keyspaces:      []string{"system", "hello", "world"},
		},
		{
			name:           "Include all * is supported (default)",
			keyspaceFilter: []string{"*"},
			keyspaces:      []string{"hello", "world"},
			expectedErr:    "",
		},
		{
			name:           "Wildcard patterns are not supported",
			keyspaceFilter: []string{"hello*"},
			keyspaces:      []string{"hello", "helloworld"},
			expectedErr:    "only existing keyspaces can be provided, but got: hello*",
		},
		{
			name:           "Wildcard patterns are not supported2",
			keyspaceFilter: []string{"*", "hi"},
			keyspaces:      []string{"hi", "hey"},
			expectedErr:    "only existing keyspaces can be provided, but got: *",
		},
		{
			name:           "Table level is not allowed",
			keyspaceFilter: []string{"hello", "system.table"},
			keyspaces:      []string{"hello", "system"},
			expectedErr:    "only existing keyspaces can be provided, but got: system.table",
		},
		{
			name:           "Exclude filters are not supported",
			keyspaceFilter: []string{"hello", "!world"},
			keyspaces:      []string{"hello", "world"},
			expectedErr:    "only existing keyspaces can be provided, but got: !world",
		},
		{
			name:           "Alternator keyspaces are supported",
			keyspaceFilter: []string{"alternator_Tab_le-With1.da_sh2-aNd.d33ot.-"},
			keyspaces:      []string{"hello", "alternator_Tab_le-With1.da_sh2-aNd.d33ot.-"},
		},
		{
			name:           "Empty(nil) filters",
			keyspaceFilter: nil,
		},
	}

	for _, tc := range testCases {
		errMsg := func(err error) string {
			if err == nil {
				return ""
			}
			return err.Error()
		}
		t.Run(tc.name, func(t *testing.T) {
			err := validateKeyspaceFilter(tc.keyspaceFilter, tc.keyspaces)
			if errMsg(err) != tc.expectedErr {
				t.Fatalf("Expected err %q, but got %q", tc.expectedErr, errMsg(err))
			}
		})
	}
}
