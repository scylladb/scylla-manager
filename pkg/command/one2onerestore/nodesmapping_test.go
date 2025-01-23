// Copyright (C) 2025 ScyllaDB

package one2onerestore

import (
	"slices"
	"testing"
)

func TestNodesMappingSet(t *testing.T) {
	testCases := []struct {
		name         string
		nodesMapping string

		expectedErr bool
		expected    nodesMapping
	}{
		{
			name:         "one line",
			nodesMapping: "./testdata/nodesmapping_oneline.txt",
			expected: nodesMapping{
				{
					Source: node{DC: "dc1", Rack: "rack1", HostID: "host1"},
					Target: node{DC: "dc2", Rack: "rack2", HostID: "host2"},
				},
			},
		},
		{
			name:         "multi line",
			nodesMapping: "./testdata/nodesmapping_multiline.txt",
			expected: nodesMapping{
				{
					Source: node{DC: "cd1", Rack: "ack1", HostID: "hwost1"},
					Target: node{DC: "dc1", Rack: "rack1", HostID: "host1"},
				},
				{
					Source: node{DC: "cd1", Rack: "ack1", HostID: "hwost2"},
					Target: node{DC: "dc1", Rack: "rack1", HostID: "host2"},
				},
				{
					Source: node{DC: "cd2", Rack: "ack2", HostID: "hwost3"},
					Target: node{DC: "dc2", Rack: "rack2", HostID: "host3"},
				},
			},
		},
		{
			name:         "whitespace",
			nodesMapping: "./testdata/nodesmapping_whitespace.txt",
			expected: nodesMapping{
				{
					Source: node{DC: "dc1", Rack: "rack1", HostID: "host1"},
					Target: node{DC: "dc2", Rack: "rack2", HostID: "host2"},
				},
			},
		},
		{
			name:         "without =",
			nodesMapping: "./testdata/nodesmapping_no=.txt",
			expected:     nodesMapping{},
			expectedErr:  true,
		},
		{
			name:         "without dc in source",
			nodesMapping: "./testdata/nodesmapping_no_dc.txt",
			expected:     nodesMapping{},
			expectedErr:  true,
		},
		{
			name:         "without dc in target",
			nodesMapping: "./testdata/nodesmapping_no_dc2.txt",
			expected:     nodesMapping{},
			expectedErr:  true,
		},
		{
			name:         "file not exists",
			nodesMapping: "./testdata/not_found.404",
			expected:     nodesMapping{},
			expectedErr:  true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			var mapping nodesMapping
			err := mapping.Set(tc.nodesMapping)
			if tc.expectedErr && err == nil {
				t.Fatalf("Expected err, got nil")
			}
			if !tc.expectedErr && err != nil {
				t.Fatalf("Unexpected err, got %v", err)
			}
			if !slices.Equal(tc.expected, mapping) {
				t.Fatalf("Expected \n\t%v\n got \n\t%v", tc.expected, mapping)
			}
		})
	}
}
