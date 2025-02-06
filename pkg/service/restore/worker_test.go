// Copyright (C) 2025 ScyllaDB
package restore

import (
	"testing"

	gocmp "github.com/google/go-cmp/cmp"
	"github.com/scylladb/scylla-manager/v3/pkg/scyllaclient"
)

func TestValidateDCMappings(t *testing.T) {
	testCases := []struct {
		name       string
		sourceDC   []string
		targetDC   []string
		dcMappings DCMappings

		expectedErr bool
	}{
		{
			name:     "sourceDC != targetDC, but with full mapping",
			sourceDC: []string{"dc1"},
			targetDC: []string{"dc2"},
			dcMappings: []DCMapping{
				{Source: "dc1", Target: "dc2"},
			},
			expectedErr: false,
		},
		{
			name:     "source != target, but will full mapping, two dcs per cluster",
			sourceDC: []string{"dc1", "dc2"},
			targetDC: []string{"dc3", "dc4"},
			dcMappings: []DCMapping{
				{Source: "dc1", Target: "dc3"},
				{Source: "dc2", Target: "dc4"},
			},
			expectedErr: false,
		},
		{
			name:     "DC mappings has unknown source dc",
			sourceDC: []string{"dc1", "dc2"},
			targetDC: []string{"dc3", "dc4"},
			dcMappings: []DCMapping{
				{Source: "dc1", Target: "dc3"},
				{Source: "dc0", Target: "dc4"},
			},
			expectedErr: true,
		},
		{
			name:     "DC mappings has unknown target dc",
			sourceDC: []string{"dc1", "dc2"},
			targetDC: []string{"dc3", "dc4"},
			dcMappings: []DCMapping{
				{Source: "dc1", Target: "dc3"},
				{Source: "dc2", Target: "dc5"},
			},
			expectedErr: true,
		},
		{
			name:     "Squeezing DCs is not supported",
			sourceDC: []string{"dc1", "dc2"},
			targetDC: []string{"dc1"},
			dcMappings: []DCMapping{
				{Source: "dc1", Target: "dc1"},
				{Source: "dc2", Target: "dc1"},
			},
			expectedErr: true,
		},
		{
			name:     "Expanding DCs is not supported",
			sourceDC: []string{"dc1"},
			targetDC: []string{"dc1", "dc2"},
			dcMappings: []DCMapping{
				{Source: "dc1", Target: "dc1"},
				{Source: "dc1", Target: "dc2"},
			},
			expectedErr: true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			w := &worker{}
			err := w.validateDCMappings(tc.dcMappings, tc.sourceDC, tc.targetDC)
			if tc.expectedErr && err == nil {
				t.Fatalf("Expected err, but got nil")
			}
			if !tc.expectedErr && err != nil {
				t.Fatalf("Unexpected err: %v", err)
			}
		})
	}
}

func TestHostsByDC(t *testing.T) {
	testCases := []struct {
		name                 string
		nodes                scyllaclient.NodeStatusInfoSlice
		targetDC2SourceDCMap map[string]string
		locationDCs          []string

		expected map[string][]string
	}{
		{
			name: "When --dc-mapping is provided will all DCs",
			nodes: scyllaclient.NodeStatusInfoSlice{
				{Addr: "n1", Datacenter: "target_dc1"},
				{Addr: "n2", Datacenter: "target_dc1"},
				{Addr: "n3", Datacenter: "target_dc2"},
				{Addr: "n4", Datacenter: "target_dc2"},
			},
			targetDC2SourceDCMap: map[string]string{
				"target_dc1": "source_dc1",
				"target_dc2": "source_dc2",
			},
			locationDCs: []string{"source_dc1", "source_dc2"},

			expected: map[string][]string{
				"source_dc1": {"n1", "n2"},
				"source_dc2": {"n3", "n4"},
			},
		},
		{
			name: "When --dc-mapping is provided will some DCs",
			nodes: scyllaclient.NodeStatusInfoSlice{
				{Addr: "n1", Datacenter: "target_dc1"},
				{Addr: "n2", Datacenter: "target_dc1"},
				{Addr: "n3", Datacenter: "target_dc2"},
				{Addr: "n4", Datacenter: "target_dc2"},
			},
			targetDC2SourceDCMap: map[string]string{
				"target_dc1": "source_dc1",
			},
			locationDCs: []string{"source_dc1", "source_dc2"},

			expected: map[string][]string{
				"source_dc1": {"n1", "n2"},
			},
		},
		{
			name: "When --dc-mapping is empty and location contains one DC",
			nodes: scyllaclient.NodeStatusInfoSlice{
				{Addr: "n1", Datacenter: "target_dc1"},
				{Addr: "n2", Datacenter: "target_dc1"},
				{Addr: "n3", Datacenter: "target_dc2"},
				{Addr: "n4", Datacenter: "target_dc2"},
			},
			targetDC2SourceDCMap: map[string]string{},
			locationDCs:          []string{"source_dc1"},

			expected: map[string][]string{
				"source_dc1": {"n1", "n2", "n3", "n4"},
			},
		},
		{
			name: "When --dc-mapping is empty and location contains multiple DCs",
			nodes: scyllaclient.NodeStatusInfoSlice{
				{Addr: "n1", Datacenter: "target_dc1"},
				{Addr: "n2", Datacenter: "target_dc1"},
				{Addr: "n3", Datacenter: "target_dc2"},
				{Addr: "n4", Datacenter: "target_dc2"},
			},
			targetDC2SourceDCMap: map[string]string{},
			locationDCs:          []string{"source_dc1", "source_dc2"},

			expected: map[string][]string{
				"source_dc1": {"n1", "n2", "n3", "n4"},
				"source_dc2": {"n1", "n2", "n3", "n4"},
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			actual := hostsByDC(tc.nodes, tc.targetDC2SourceDCMap, tc.locationDCs)
			if diff := gocmp.Diff(actual, tc.expected); diff != "" {
				t.Fatalf("Unexpected result: %v", diff)
			}
		})
	}
}
