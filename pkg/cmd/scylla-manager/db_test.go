// Copyright (C) 2026 ScyllaDB

package main

import (
	"testing"

	"github.com/google/go-cmp/cmp"
)

func TestChooseDCRF(t *testing.T) {
	testCases := []struct {
		name     string
		dcs      map[string]dcInfo
		expected map[string]int
	}{
		{
			name: "1 dc 1 rack 1 node",
			dcs: map[string]dcInfo{
				"dc1": {
					dc:    "dc1",
					nodes: 1,
					racks: map[string]rackInfo{
						"r1": {dc: "dc1", rack: "r1", nodes: 1},
					},
				},
			},
			expected: map[string]int{
				"dc1": 1,
			},
		},
		{
			name: "1 dc 1 rack 2 nodes",
			dcs: map[string]dcInfo{
				"dc1": {
					dc:    "dc1",
					nodes: 2,
					racks: map[string]rackInfo{
						"r1": {dc: "dc1", rack: "r1", nodes: 2},
					},
				},
			},
			expected: map[string]int{
				"dc1": 2,
			},
		},
		{
			name: "1 dc 1 rack 3 nodes",
			dcs: map[string]dcInfo{
				"dc1": {
					dc:    "dc1",
					nodes: 3,
					racks: map[string]rackInfo{
						"r1": {dc: "dc1", rack: "r1", nodes: 3},
					},
				},
			},
			expected: map[string]int{
				"dc1": 3,
			},
		},
		{
			name: "1 dc 1 rack 5 nodes",
			dcs: map[string]dcInfo{
				"dc1": {
					dc:    "dc1",
					nodes: 5,
					racks: map[string]rackInfo{
						"r1": {dc: "dc1", rack: "r1", nodes: 5},
					},
				},
			},
			expected: map[string]int{
				"dc1": 3,
			},
		},
		{
			name: "1 dc 2 racks 1 node each",
			dcs: map[string]dcInfo{
				"dc1": {
					dc:    "dc1",
					nodes: 2,
					racks: map[string]rackInfo{
						"r1": {dc: "dc1", rack: "r1", nodes: 1},
						"r2": {dc: "dc1", rack: "r2", nodes: 1},
					},
				},
			},
			expected: map[string]int{
				"dc1": 2,
			},
		},
		{
			name: "1 dc 2 racks 2 nodes each",
			dcs: map[string]dcInfo{
				"dc1": {
					dc:    "dc1",
					nodes: 4,
					racks: map[string]rackInfo{
						"r1": {dc: "dc1", rack: "r1", nodes: 2},
						"r2": {dc: "dc1", rack: "r2", nodes: 2},
					},
				},
			},
			expected: map[string]int{
				"dc1": 3,
			},
		},
		{
			name: "2 dc 1 rack each 1 node each",
			dcs: map[string]dcInfo{
				"dc1": {
					dc:    "dc1",
					nodes: 1,
					racks: map[string]rackInfo{
						"r1": {dc: "dc1", rack: "r1", nodes: 1},
					},
				},
				"dc2": {
					dc:    "dc2",
					nodes: 1,
					racks: map[string]rackInfo{
						"r1": {dc: "dc2", rack: "r1", nodes: 1},
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
			dcs: map[string]dcInfo{
				"dc1": {
					dc:    "dc1",
					nodes: 3,
					racks: map[string]rackInfo{
						"r1": {dc: "dc1", rack: "r1", nodes: 3},
					},
				},
				"dc2": {
					dc:    "dc2",
					nodes: 1,
					racks: map[string]rackInfo{
						"r1": {dc: "dc2", rack: "r1", nodes: 1},
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
			dcs: map[string]dcInfo{
				"dc1": {
					dc:    "dc1",
					nodes: 4,
					racks: map[string]rackInfo{
						"r1": {dc: "dc1", rack: "r1", nodes: 2},
						"r2": {dc: "dc1", rack: "r2", nodes: 2},
					},
				},
				"dc2": {
					dc:    "dc2",
					nodes: 2,
					racks: map[string]rackInfo{
						"r1": {dc: "dc2", rack: "r1", nodes: 2},
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
			dcs: map[string]dcInfo{
				"dc1": {
					dc:    "dc1",
					nodes: 2,
					racks: map[string]rackInfo{
						"r1": {dc: "dc1", rack: "r1", nodes: 1},
						"r2": {dc: "dc1", rack: "r2", nodes: 1},
					},
				},
				"dc2": {
					dc:    "dc2",
					nodes: 4,
					racks: map[string]rackInfo{
						"r1": {dc: "dc2", rack: "r1", nodes: 2},
						"r2": {dc: "dc2", rack: "r2", nodes: 2},
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
			dcs: map[string]dcInfo{
				"dc1": {
					dc:    "dc1",
					nodes: 1,
					racks: map[string]rackInfo{
						"r1": {dc: "dc1", rack: "r1", nodes: 1},
					},
				},
				"dc2": {
					dc:    "dc2",
					nodes: 2,
					racks: map[string]rackInfo{
						"r1": {dc: "dc2", rack: "r1", nodes: 2},
					},
				},
				"dc3": {
					dc:    "dc3",
					nodes: 3,
					racks: map[string]rackInfo{
						"r1": {dc: "dc3", rack: "r1", nodes: 3},
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
			dcs: map[string]dcInfo{
				"dc1": {
					dc:    "dc1",
					nodes: 22,
					racks: map[string]rackInfo{
						"r1": {dc: "dc1", rack: "r1", nodes: 5},
						"r2": {dc: "dc1", rack: "r2", nodes: 7},
						"r3": {dc: "dc1", rack: "r3", nodes: 10},
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
