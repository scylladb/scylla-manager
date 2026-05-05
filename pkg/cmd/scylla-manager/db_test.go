// Copyright (C) 2026 ScyllaDB

package main

import (
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/scylladb/scylla-manager/v3/pkg/util2/topology"
)

func TestChooseDCRF(t *testing.T) {
	testCases := []struct {
		name     string
		dcs      topology.ClusterTopology
		expected map[string]int
	}{
		{
			name: "1 dc 1 rack 1 node",
			dcs: topology.ClusterTopology{
				DCs: map[string]topology.DCTopology{
					"dc1": {
						DC:    "dc1",
						Nodes: 1,
						Racks: map[string]topology.RackTopology{
							"r1": {DC: "dc1", Rack: "r1", Nodes: 1},
						},
					},
				},
			},
			expected: map[string]int{
				"dc1": 1,
			},
		},
		{
			name: "1 dc 1 rack 2 nodes",
			dcs: topology.ClusterTopology{
				DCs: map[string]topology.DCTopology{
					"dc1": {
						DC:    "dc1",
						Nodes: 2,
						Racks: map[string]topology.RackTopology{
							"r1": {DC: "dc1", Rack: "r1", Nodes: 2},
						},
					},
				},
			},
			expected: map[string]int{
				"dc1": 2,
			},
		},
		{
			name: "1 dc 1 rack 3 nodes",
			dcs: topology.ClusterTopology{
				DCs: map[string]topology.DCTopology{
					"dc1": {
						DC:    "dc1",
						Nodes: 3,
						Racks: map[string]topology.RackTopology{
							"r1": {DC: "dc1", Rack: "r1", Nodes: 3},
						},
					},
				},
			},
			expected: map[string]int{
				"dc1": 3,
			},
		},
		{
			name: "1 dc 1 rack 5 nodes",
			dcs: topology.ClusterTopology{
				DCs: map[string]topology.DCTopology{
					"dc1": {
						DC:    "dc1",
						Nodes: 5,
						Racks: map[string]topology.RackTopology{
							"r1": {DC: "dc1", Rack: "r1", Nodes: 5},
						},
					},
				},
			},
			expected: map[string]int{
				"dc1": 3,
			},
		},
		{
			name: "1 dc 2 racks 1 node each",
			dcs: topology.ClusterTopology{
				DCs: map[string]topology.DCTopology{
					"dc1": {
						DC:    "dc1",
						Nodes: 2,
						Racks: map[string]topology.RackTopology{
							"r1": {DC: "dc1", Rack: "r1", Nodes: 1},
							"r2": {DC: "dc1", Rack: "r2", Nodes: 1},
						},
					},
				},
			},
			expected: map[string]int{
				"dc1": 2,
			},
		},
		{
			name: "1 dc 2 racks 2 nodes each",
			dcs: topology.ClusterTopology{
				DCs: map[string]topology.DCTopology{
					"dc1": {
						DC:    "dc1",
						Nodes: 4,
						Racks: map[string]topology.RackTopology{
							"r1": {DC: "dc1", Rack: "r1", Nodes: 2},
							"r2": {DC: "dc1", Rack: "r2", Nodes: 2},
						},
					},
				},
			},
			expected: map[string]int{
				"dc1": 3,
			},
		},
		{
			name: "2 dc 1 rack each 1 node each",
			dcs: topology.ClusterTopology{
				DCs: map[string]topology.DCTopology{
					"dc1": {
						DC:    "dc1",
						Nodes: 1,
						Racks: map[string]topology.RackTopology{
							"r1": {DC: "dc1", Rack: "r1", Nodes: 1},
						},
					},
					"dc2": {
						DC:    "dc2",
						Nodes: 1,
						Racks: map[string]topology.RackTopology{
							"r1": {DC: "dc2", Rack: "r1", Nodes: 1},
						},
					},
				},
			},
			expected: map[string]int{
				"dc1": 1,
				"dc2": 1,
			},
		},
		{
			name: "2 dc 1 rack each 3 nodes and 1 node",
			dcs: topology.ClusterTopology{
				DCs: map[string]topology.DCTopology{
					"dc1": {
						DC:    "dc1",
						Nodes: 3,
						Racks: map[string]topology.RackTopology{
							"r1": {DC: "dc1", Rack: "r1", Nodes: 3},
						},
					},
					"dc2": {
						DC:    "dc2",
						Nodes: 1,
						Racks: map[string]topology.RackTopology{
							"r1": {DC: "dc2", Rack: "r1", Nodes: 1},
						},
					},
				},
			},
			expected: map[string]int{
				"dc1": 2,
				"dc2": 1,
			},
		},
		{
			name: "2 dc 2 racks and 1 rack",
			dcs: topology.ClusterTopology{
				DCs: map[string]topology.DCTopology{
					"dc1": {
						DC:    "dc1",
						Nodes: 4,
						Racks: map[string]topology.RackTopology{
							"r1": {DC: "dc1", Rack: "r1", Nodes: 2},
							"r2": {DC: "dc1", Rack: "r2", Nodes: 2},
						},
					},
					"dc2": {
						DC:    "dc2",
						Nodes: 2,
						Racks: map[string]topology.RackTopology{
							"r1": {DC: "dc2", Rack: "r1", Nodes: 2},
						},
					},
				},
			},
			expected: map[string]int{
				"dc1": 2,
				"dc2": 1,
			},
		},
		{
			name: "2 dc 2 racks each",
			dcs: topology.ClusterTopology{
				DCs: map[string]topology.DCTopology{
					"dc1": {
						DC:    "dc1",
						Nodes: 2,
						Racks: map[string]topology.RackTopology{
							"r1": {DC: "dc1", Rack: "r1", Nodes: 1},
							"r2": {DC: "dc1", Rack: "r2", Nodes: 1},
						},
					},
					"dc2": {
						DC:    "dc2",
						Nodes: 4,
						Racks: map[string]topology.RackTopology{
							"r1": {DC: "dc2", Rack: "r1", Nodes: 2},
							"r2": {DC: "dc2", Rack: "r2", Nodes: 2},
						},
					},
				},
			},
			expected: map[string]int{
				"dc1": 2,
				"dc2": 2,
			},
		},
		{
			name: "3 dc 1 rack each",
			dcs: topology.ClusterTopology{
				DCs: map[string]topology.DCTopology{
					"dc1": {
						DC:    "dc1",
						Nodes: 1,
						Racks: map[string]topology.RackTopology{
							"r1": {DC: "dc1", Rack: "r1", Nodes: 1},
						},
					},
					"dc2": {
						DC:    "dc2",
						Nodes: 2,
						Racks: map[string]topology.RackTopology{
							"r1": {DC: "dc2", Rack: "r1", Nodes: 2},
						},
					},
					"dc3": {
						DC:    "dc3",
						Nodes: 3,
						Racks: map[string]topology.RackTopology{
							"r1": {DC: "dc3", Rack: "r1", Nodes: 3},
						},
					},
				},
			},
			expected: map[string]int{
				"dc1": 1,
				"dc2": 1,
				"dc3": 1,
			},
		},
		{
			name: "1 dc 3 racks",
			dcs: topology.ClusterTopology{
				DCs: map[string]topology.DCTopology{
					"dc1": {
						DC:    "dc1",
						Nodes: 22,
						Racks: map[string]topology.RackTopology{
							"r1": {DC: "dc1", Rack: "r1", Nodes: 5},
							"r2": {DC: "dc1", Rack: "r2", Nodes: 7},
							"r3": {DC: "dc1", Rack: "r3", Nodes: 10},
						},
					},
				},
			},
			expected: map[string]int{
				"dc1": 3,
			},
		},
	}

	for _, tt := range testCases {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got := chooseDCRF(tt.dcs)
			if diff := cmp.Diff(tt.expected, got); diff != "" {
				t.Fatal(diff)
			}
		})
	}
}
