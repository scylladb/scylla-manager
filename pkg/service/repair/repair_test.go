// Copyright (C) 2024 ScyllaDB

package repair_test

import (
	"fmt"
	"testing"

	"github.com/scylladb/scylla-manager/v3/pkg/scyllaclient"
	"github.com/scylladb/scylla-manager/v3/pkg/service/repair"
)

func TestMaxRingParallel(t *testing.T) {
	hostDC := map[string]string{
		// dc1 -> 3
		"h1": "dc1",
		"h2": "dc1",
		"h3": "dc1",
		// dc2 -> 4
		"h4": "dc2",
		"h5": "dc2",
		"h6": "dc2",
		"h7": "dc2",
		// dc3 -> 5
		"h8":  "dc3",
		"h9":  "dc3",
		"h10": "dc3",
		"h11": "dc3",
		"h12": "dc3",
	}

	testCases := []struct {
		Ring     scyllaclient.Ring
		DCs      []string
		Expected int
	}{
		{
			Ring: scyllaclient.Ring{
				HostDC:      hostDC,
				Replication: scyllaclient.SimpleStrategy,
				RF:          4,
			},
			DCs:      []string{"dc1", "dc2", "dc3"},
			Expected: 3,
		},
		{
			Ring: scyllaclient.Ring{
				HostDC:      hostDC,
				Replication: scyllaclient.SimpleStrategy,
				RF:          3,
			},
			DCs:      []string{"dc1", "dc2"},
			Expected: 2,
		},
		{
			Ring: scyllaclient.Ring{
				HostDC:      hostDC,
				Replication: scyllaclient.NetworkTopologyStrategy,
				RF:          5,
				DCrf: map[string]int{
					"dc1": 1,
					"dc2": 2,
					"dc3": 2,
				},
			},
			DCs:      []string{"dc1", "dc2", "dc3"},
			Expected: 2,
		},
		{
			Ring: scyllaclient.Ring{
				HostDC:      hostDC,
				Replication: scyllaclient.NetworkTopologyStrategy,
				RF:          8,
				DCrf: map[string]int{
					"dc1": 1,
					"dc2": 2,
					"dc3": 5,
				},
			},
			DCs:      []string{"dc1", "dc2"},
			Expected: 2,
		},
		{
			Ring: scyllaclient.Ring{
				HostDC:      hostDC,
				Replication: scyllaclient.NetworkTopologyStrategy,
				RF:          4,
				DCrf: map[string]int{
					"dc1": 2,
					"dc2": 1,
					"dc3": 1,
				},
			},
			DCs:      []string{"dc1", "dc3"},
			Expected: 1,
		},
	}

	for i := range testCases {
		tc := testCases[i]
		t.Run("test "+fmt.Sprint(i), func(t *testing.T) {
			t.Parallel()
			if out := repair.MaxRingParallel(tc.Ring, tc.DCs); out != tc.Expected {
				t.Fatalf("Expected %d, got %d", tc.Expected, out)
			}
		})
	}
}

func TestShouldRepairRing(t *testing.T) {
	hostDC := map[string]string{
		// dc1 -> 1
		"h1": "dc1",
		// dc2 -> 1
		"h2": "dc2",
		// dc3 -> 3
		"h3": "dc3",
		"h4": "dc3",
		"h5": "dc3",
		// dc3 -> 4
		"h6": "dc4",
		"h7": "dc4",
		"h8": "dc4",
		"h9": "dc4",
	}

	testCases := []struct {
		Ring     scyllaclient.Ring
		DCs      []string
		Host     string
		Expected bool
	}{
		{
			Ring: scyllaclient.Ring{
				HostDC:      hostDC,
				Replication: scyllaclient.LocalStrategy,
				RF:          1,
			},
			DCs:      []string{"dc1", "dc2", "dc3", "dc4"},
			Expected: false,
		},
		{
			Ring: scyllaclient.Ring{
				HostDC:      hostDC,
				Replication: scyllaclient.SimpleStrategy,
				RF:          1,
			},
			DCs:      []string{"dc1", "dc2", "dc3", "dc4"},
			Expected: false,
		},
		{
			Ring: scyllaclient.Ring{
				HostDC:      hostDC,
				Replication: scyllaclient.SimpleStrategy,
				RF:          2,
			},
			DCs:      []string{"dc1", "dc2", "dc3", "dc4"},
			Expected: true,
		},
		{
			Ring: scyllaclient.Ring{
				HostDC:      hostDC,
				Replication: scyllaclient.SimpleStrategy,
				RF:          3,
			},
			DCs:      []string{"dc3", "dc4"},
			Expected: false,
		},
		{
			Ring: scyllaclient.Ring{
				HostDC:      hostDC,
				Replication: scyllaclient.SimpleStrategy,
				RF:          4,
			},
			DCs:      []string{"dc3", "dc4"},
			Expected: true,
		},
		{
			Ring: scyllaclient.Ring{
				HostDC:      hostDC,
				Replication: scyllaclient.SimpleStrategy,
				RF:          4,
			},
			DCs:      []string{"dc4"},
			Expected: false,
		},
		{
			Ring: scyllaclient.Ring{
				HostDC:      hostDC,
				Replication: scyllaclient.NetworkTopologyStrategy,
				RF:          4,
				DCrf: map[string]int{
					"dc1": 1,
					"dc2": 1,
					"dc3": 1,
					"dc4": 1,
				},
			},
			DCs:      []string{"dc1", "dc2", "dc3", "dc4"},
			Expected: true,
		},
		{
			Ring: scyllaclient.Ring{
				HostDC:      hostDC,
				Replication: scyllaclient.NetworkTopologyStrategy,
				RF:          4,
				DCrf: map[string]int{
					"dc1": 1,
					"dc2": 1,
					"dc3": 1,
					"dc4": 1,
				},
			},
			DCs:      []string{"dc1"},
			Expected: false,
		},
		{
			Ring: scyllaclient.Ring{
				HostDC:      hostDC,
				Replication: scyllaclient.NetworkTopologyStrategy,
				RF:          8,
				DCrf: map[string]int{
					"dc1": 1,
					"dc2": 1,
					"dc3": 2,
					"dc4": 2,
				},
			},
			DCs:      []string{"dc3"},
			Expected: true,
		},
		{
			Ring: scyllaclient.Ring{
				HostDC:      hostDC,
				Replication: scyllaclient.NetworkTopologyStrategy,
				RF:          8,
				DCrf: map[string]int{
					"dc1": 1,
					"dc2": 1,
					"dc3": 3,
					"dc4": 4,
				},
			},
			DCs:      []string{"dc4"},
			Host:     "h6",
			Expected: true,
		},
		{
			Ring: scyllaclient.Ring{
				HostDC:      hostDC,
				Replication: scyllaclient.NetworkTopologyStrategy,
				RF:          8,
				DCrf: map[string]int{
					"dc1": 1,
					"dc2": 1,
					"dc3": 3,
					"dc4": 4,
				},
			},
			DCs:      []string{"dc4"},
			Host:     "h2",
			Expected: false,
		},
		{
			Ring: scyllaclient.Ring{
				HostDC:      hostDC,
				Replication: scyllaclient.NetworkTopologyStrategy,
				RF:          8,
				DCrf: map[string]int{
					"dc1": 1,
					"dc2": 1,
					"dc3": 3,
					"dc4": 4,
				},
			},
			DCs:      []string{"dc1", "dc2", "dc3", "dc4"},
			Host:     "h1",
			Expected: true,
		},
	}

	for i := range testCases {
		tc := testCases[i]
		t.Run("test "+fmt.Sprint(i), func(t *testing.T) {
			t.Parallel()
			if out := repair.ShouldRepairRing(tc.Ring, tc.DCs, tc.Host); out != tc.Expected {
				t.Fatalf("Expected %v, got %v", tc.Expected, out)
			}
		})
	}
}
