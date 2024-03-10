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
