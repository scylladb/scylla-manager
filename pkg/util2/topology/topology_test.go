// Copyright (C) 2026 ScyllaDB

package topology

import (
	"iter"
	"testing"
)

func TestClusterTopologyContainsDCsAndRacks(t *testing.T) {
	testCases := []struct {
		name     string
		ct       ClusterTopology
		other    ClusterTopology
		expected bool
	}{
		{
			name:     "both empty",
			ct:       ClusterTopology{DCs: map[string]DCTopology{}},
			other:    ClusterTopology{DCs: map[string]DCTopology{}},
			expected: true,
		},
		{
			name: "other empty",
			ct: ClusterTopology{DCs: map[string]DCTopology{
				"dc1": {DC: "dc1", Racks: map[string]RackTopology{
					"rack1": {DC: "dc1", Rack: "rack1"},
				}},
			}},
			other:    ClusterTopology{DCs: map[string]DCTopology{}},
			expected: true,
		},
		{
			name: "identical single dc",
			ct: ClusterTopology{DCs: map[string]DCTopology{
				"dc1": {DC: "dc1", Racks: map[string]RackTopology{
					"rack1": {DC: "dc1", Rack: "rack1"},
				}},
			}},
			other: ClusterTopology{DCs: map[string]DCTopology{
				"dc1": {DC: "dc1", Racks: map[string]RackTopology{
					"rack1": {DC: "dc1", Rack: "rack1"},
				}},
			}},
			expected: true,
		},
		{
			name: "ct has extra dc",
			ct: ClusterTopology{DCs: map[string]DCTopology{
				"dc1": {DC: "dc1", Racks: map[string]RackTopology{
					"rack1": {DC: "dc1", Rack: "rack1"},
				}},
				"dc2": {DC: "dc2", Racks: map[string]RackTopology{
					"rack1": {DC: "dc2", Rack: "rack1"},
				}},
			}},
			other: ClusterTopology{DCs: map[string]DCTopology{
				"dc1": {DC: "dc1", Racks: map[string]RackTopology{
					"rack1": {DC: "dc1", Rack: "rack1"},
				}},
			}},
			expected: true,
		},
		{
			name: "other has extra dc",
			ct: ClusterTopology{DCs: map[string]DCTopology{
				"dc1": {DC: "dc1", Racks: map[string]RackTopology{
					"rack1": {DC: "dc1", Rack: "rack1"},
				}},
			}},
			other: ClusterTopology{DCs: map[string]DCTopology{
				"dc1": {DC: "dc1", Racks: map[string]RackTopology{
					"rack1": {DC: "dc1", Rack: "rack1"},
				}},
				"dc2": {DC: "dc2", Racks: map[string]RackTopology{
					"rack1": {DC: "dc2", Rack: "rack1"},
				}},
			}},
			expected: false,
		},
		{
			name: "same dc different racks",
			ct: ClusterTopology{DCs: map[string]DCTopology{
				"dc1": {DC: "dc1", Racks: map[string]RackTopology{
					"rack1": {DC: "dc1", Rack: "rack1"},
				}},
			}},
			other: ClusterTopology{DCs: map[string]DCTopology{
				"dc1": {DC: "dc1", Racks: map[string]RackTopology{
					"rack2": {DC: "dc1", Rack: "rack2"},
				}},
			}},
			expected: false,
		},
		{
			name: "other has extra rack in shared dc",
			ct: ClusterTopology{DCs: map[string]DCTopology{
				"dc1": {DC: "dc1", Racks: map[string]RackTopology{
					"rack1": {DC: "dc1", Rack: "rack1"},
				}},
			}},
			other: ClusterTopology{DCs: map[string]DCTopology{
				"dc1": {DC: "dc1", Racks: map[string]RackTopology{
					"rack1": {DC: "dc1", Rack: "rack1"},
					"rack2": {DC: "dc1", Rack: "rack2"},
				}},
			}},
			expected: false,
		},
		{
			name: "ct has extra rack in shared dc",
			ct: ClusterTopology{DCs: map[string]DCTopology{
				"dc1": {DC: "dc1", Racks: map[string]RackTopology{
					"rack1": {DC: "dc1", Rack: "rack1"},
					"rack2": {DC: "dc1", Rack: "rack2"},
				}},
			}},
			other: ClusterTopology{DCs: map[string]DCTopology{
				"dc1": {DC: "dc1", Racks: map[string]RackTopology{
					"rack1": {DC: "dc1", Rack: "rack1"},
				}},
			}},
			expected: false,
		},
		{
			name: "different node counts are ignored",
			ct: ClusterTopology{DCs: map[string]DCTopology{
				"dc1": {DC: "dc1", Nodes: 3, Racks: map[string]RackTopology{
					"rack1": {DC: "dc1", Rack: "rack1", Nodes: 3},
				}},
			}},
			other: ClusterTopology{DCs: map[string]DCTopology{
				"dc1": {DC: "dc1", Nodes: 5, Racks: map[string]RackTopology{
					"rack1": {DC: "dc1", Rack: "rack1", Nodes: 5},
				}},
			}},
			expected: true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if got := tc.ct.ContainsDCsAndRacks(tc.other); got != tc.expected {
				t.Errorf("ContainsDCsAndRacks() = %v, want %v", got, tc.expected)
			}
		})
	}
}

// dcRackPair is a helper struct for building test iterators.
type dcRackPair struct {
	DC   string
	Rack string
}

// pairsIter returns an iter.Seq2 that yields dc/rack pairs in order.
func pairsIter(pairs []dcRackPair) iter.Seq2[string, string] {
	return func(yield func(string, string) bool) {
		for _, p := range pairs {
			if !yield(p.DC, p.Rack) {
				return
			}
		}
	}
}

func TestBuildClusterTopology(t *testing.T) {
	testCases := []struct {
		name     string
		pairs    []dcRackPair
		expected ClusterTopology
	}{
		{
			name:  "empty iterator",
			pairs: nil,
			expected: ClusterTopology{
				DCs: map[string]DCTopology{},
			},
		},
		{
			name: "single node",
			pairs: []dcRackPair{
				{DC: "dc1", Rack: "rack1"},
			},
			expected: ClusterTopology{DCs: map[string]DCTopology{
				"dc1": {DC: "dc1", Nodes: 1, Racks: map[string]RackTopology{
					"rack1": {DC: "dc1", Rack: "rack1", Nodes: 1},
				}},
			}},
		},
		{
			name: "single dc single rack multiple nodes",
			pairs: []dcRackPair{
				{DC: "dc1", Rack: "rack1"},
				{DC: "dc1", Rack: "rack1"},
				{DC: "dc1", Rack: "rack1"},
			},
			expected: ClusterTopology{DCs: map[string]DCTopology{
				"dc1": {DC: "dc1", Nodes: 3, Racks: map[string]RackTopology{
					"rack1": {DC: "dc1", Rack: "rack1", Nodes: 3},
				}},
			}},
		},
		{
			name: "single dc multiple racks",
			pairs: []dcRackPair{
				{DC: "dc1", Rack: "rack1"},
				{DC: "dc1", Rack: "rack2"},
				{DC: "dc1", Rack: "rack1"},
				{DC: "dc1", Rack: "rack2"},
			},
			expected: ClusterTopology{DCs: map[string]DCTopology{
				"dc1": {DC: "dc1", Nodes: 4, Racks: map[string]RackTopology{
					"rack1": {DC: "dc1", Rack: "rack1", Nodes: 2},
					"rack2": {DC: "dc1", Rack: "rack2", Nodes: 2},
				}},
			}},
		},
		{
			name: "multiple dcs single rack each",
			pairs: []dcRackPair{
				{DC: "dc1", Rack: "rack1"},
				{DC: "dc2", Rack: "rack1"},
				{DC: "dc1", Rack: "rack1"},
			},
			expected: ClusterTopology{DCs: map[string]DCTopology{
				"dc1": {DC: "dc1", Nodes: 2, Racks: map[string]RackTopology{
					"rack1": {DC: "dc1", Rack: "rack1", Nodes: 2},
				}},
				"dc2": {DC: "dc2", Nodes: 1, Racks: map[string]RackTopology{
					"rack1": {DC: "dc2", Rack: "rack1", Nodes: 1},
				}},
			}},
		},
		{
			name: "multiple dcs multiple racks",
			pairs: []dcRackPair{
				{DC: "dc1", Rack: "rack1"},
				{DC: "dc1", Rack: "rack2"},
				{DC: "dc2", Rack: "rack1"},
				{DC: "dc2", Rack: "rack2"},
				{DC: "dc2", Rack: "rack3"},
				{DC: "dc1", Rack: "rack1"},
			},
			expected: ClusterTopology{DCs: map[string]DCTopology{
				"dc1": {DC: "dc1", Nodes: 3, Racks: map[string]RackTopology{
					"rack1": {DC: "dc1", Rack: "rack1", Nodes: 2},
					"rack2": {DC: "dc1", Rack: "rack2", Nodes: 1},
				}},
				"dc2": {DC: "dc2", Nodes: 3, Racks: map[string]RackTopology{
					"rack1": {DC: "dc2", Rack: "rack1", Nodes: 1},
					"rack2": {DC: "dc2", Rack: "rack2", Nodes: 1},
					"rack3": {DC: "dc2", Rack: "rack3", Nodes: 1},
				}},
			}},
		},
		{
			name: "three dcs with varying node counts",
			pairs: []dcRackPair{
				{DC: "us-east", Rack: "a"},
				{DC: "us-east", Rack: "b"},
				{DC: "us-east", Rack: "a"},
				{DC: "eu-west", Rack: "a"},
				{DC: "ap-south", Rack: "a"},
				{DC: "ap-south", Rack: "a"},
			},
			expected: ClusterTopology{DCs: map[string]DCTopology{
				"us-east": {DC: "us-east", Nodes: 3, Racks: map[string]RackTopology{
					"a": {DC: "us-east", Rack: "a", Nodes: 2},
					"b": {DC: "us-east", Rack: "b", Nodes: 1},
				}},
				"eu-west": {DC: "eu-west", Nodes: 1, Racks: map[string]RackTopology{
					"a": {DC: "eu-west", Rack: "a", Nodes: 1},
				}},
				"ap-south": {DC: "ap-south", Nodes: 2, Racks: map[string]RackTopology{
					"a": {DC: "ap-south", Rack: "a", Nodes: 2},
				}},
			}},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got := BuildClusterTopology(pairsIter(tc.pairs))
			if !got.ContainsDCsAndRacks(tc.expected) || !tc.expected.ContainsDCsAndRacks(got) {
				t.Errorf("BuildClusterTopology() = %+v, want %+v", got, tc.expected)
			}
		})
	}
}
