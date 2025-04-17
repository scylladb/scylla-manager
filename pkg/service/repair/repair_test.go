// Copyright (C) 2024 ScyllaDB

package repair_test

import (
	"fmt"
	"net/netip"
	"strings"
	"testing"

	"github.com/scylladb/scylla-manager/v3/pkg/dht"
	"github.com/scylladb/scylla-manager/v3/pkg/scyllaclient"
	"github.com/scylladb/scylla-manager/v3/pkg/service/repair"
	"github.com/scylladb/scylla-manager/v3/pkg/util2/slices"
)

func TestMaxRingParallel(t *testing.T) {
	hostDC := map[netip.Addr]string{
		// dc1 -> 3
		hostIP("h1"): "dc1",
		hostIP("h2"): "dc1",
		hostIP("h3"): "dc1",
		// dc2 -> 4
		hostIP("h4"): "dc2",
		hostIP("5"):  "dc2",
		hostIP("6"):  "dc2",
		hostIP("7"):  "dc2",
		// dc3 -> 5
		hostIP("8"):  "dc3",
		hostIP("9"):  "dc3",
		hostIP("10"): "dc3",
		hostIP("11"): "dc3",
		hostIP("12"): "dc3",
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
	hostDC := map[netip.Addr]string{
		// dc1 -> 1
		hostIP("h1"): "dc1",
		// dc2 -> 1
		hostIP("h2"): "dc2",
		// dc3 -> 3
		hostIP("h3"): "dc3",
		hostIP("h4"): "dc3",
		hostIP("h5"): "dc3",
		// dc3 -> 4
		hostIP("h6"): "dc4",
		hostIP("h7"): "dc4",
		hostIP("h8"): "dc4",
		hostIP("h9"): "dc4",
	}

	rs := func(reps ...string) scyllaclient.ReplicaTokenRanges {
		return scyllaclient.ReplicaTokenRanges{
			ReplicaSet: slices.Map(reps, hostIP),
			Ranges: []scyllaclient.TokenRange{
				{
					StartToken: dht.Murmur3MinToken,
					EndToken:   dht.Murmur3MaxToken,
				},
			},
		}
	}

	allDCs := []string{"dc1", "dc2", "dc3", "dc4"}

	testCases := []struct {
		Name     string
		Ring     scyllaclient.Ring
		DCs      []string
		Host     string
		Expected bool
	}{
		{
			Name: "LocalStrategy",
			Ring: scyllaclient.Ring{
				ReplicaTokens: []scyllaclient.ReplicaTokenRanges{rs("h1")},
				HostDC:        hostDC,
			},
			DCs:      allDCs,
			Expected: false, // can't repair local keyspace
		},
		{
			Name: "SimpleStrategy{rf=1}",
			Ring: scyllaclient.Ring{
				ReplicaTokens: []scyllaclient.ReplicaTokenRanges{
					rs("h1"), rs("h2"), rs("h3"),
					rs("h4"), rs("h5"), rs("h6"),
					rs("h7"), rs("h8"), rs("h9"),
				},
				HostDC: hostDC,
			},
			DCs:      allDCs,
			Expected: false, // can't repair rf=1
		},
		{
			Name: "SimpleStrategy{rf=2}",
			Ring: scyllaclient.Ring{
				ReplicaTokens: []scyllaclient.ReplicaTokenRanges{
					rs("h1", "h2"), rs("h2", "h3"), rs("h3", "h4"),
					rs("h4", "h5"), rs("h5", "h6"), rs("h6", "h7"),
					rs("h7", "h8"), rs("h8", "h9"), rs("h9", "h1"),
				},
				HostDC: hostDC,
			},
			DCs:      allDCs,
			Expected: true,
		},
		{
			Name: "--dc 'dc3,dc4', SimpleStrategy{rf=3}",
			Ring: scyllaclient.Ring{
				ReplicaTokens: []scyllaclient.ReplicaTokenRanges{
					rs("h1", "h2", "h3"), rs("h2", "h3", "h4"), rs("h3", "h4", "h5"),
					rs("h4", "h5", "h6"), rs("h5", "h6", "h7"), rs("h6", "h7", "h8"),
					rs("h7", "h8", "h9"), rs("h8", "h9", "h1"), rs("h9", "h1", "h2"),
				},
				HostDC: hostDC,
			},
			DCs:      []string{"dc3", "dc4"},
			Expected: false, // rs("h1", "h2", "h3") has 1 replica in dc3 and dc4
		},
		{
			Name: "--dc 'dc3,dc4', SimpleStrategy{rf=4}",
			Ring: scyllaclient.Ring{
				ReplicaTokens: []scyllaclient.ReplicaTokenRanges{
					rs("h1", "h2", "h3", "h4"), rs("h2", "h3", "h4", "h5"), rs("h3", "h4", "h5", "h6"),
					rs("h4", "h5", "h6", "h7"), rs("h5", "h6", "h7", "h8"), rs("h6", "h7", "h8", "h9"),
					rs("h7", "h8", "h9", "h1"), rs("h8", "h9", "h1", "h2"), rs("h9", "h1", "h2", "h3"),
				},
				HostDC: hostDC,
			},
			DCs:      []string{"dc3", "dc4"},
			Expected: true,
		},
		{
			Name: "--dc 'dc4', SimpleStrategy{rf=4}",
			Ring: scyllaclient.Ring{
				ReplicaTokens: []scyllaclient.ReplicaTokenRanges{
					rs("h1", "h2", "h3", "h4"), rs("h2", "h3", "h4", "h5"), rs("h3", "h4", "h5", "h6"),
					rs("h4", "h5", "h6", "h7"), rs("h5", "h6", "h7", "h8"), rs("h6", "h7", "h8", "h9"),
					rs("h7", "h8", "h9", "h1"), rs("h8", "h9", "h1", "h2"), rs("h9", "h1", "h2", "h3"),
				},
				HostDC: hostDC,
			},
			DCs:      []string{"dc4"},
			Expected: false, // rs("h1", "h2", "h3", "h4") has 0 replicas in dc4
		},
		{
			Name: "NetworkTopologyStrategy{'dc1'=1, 'dc2'=1, 'dc3'=1, 'dc4'=1}",
			Ring: scyllaclient.Ring{
				ReplicaTokens: []scyllaclient.ReplicaTokenRanges{
					rs("h1", "h2", "h3", "h6"), rs("h1", "h2", "h4", "h6"), rs("h1", "h2", "h5", "h6"),
					rs("h1", "h2", "h3", "h7"), rs("h1", "h2", "h4", "h7"), rs("h1", "h2", "h5", "h7"),
					rs("h1", "h2", "h3", "h8"), rs("h1", "h2", "h4", "h8"), rs("h1", "h2", "h5", "h8"),
					rs("h1", "h2", "h3", "h9"), rs("h1", "h2", "h4", "h9"), rs("h1", "h2", "h5", "h9"),
				},
				HostDC: hostDC,
			},
			DCs:      allDCs,
			Expected: true,
		},
		{
			Name: "--dc 'dc1', NetworkTopologyStrategy{'dc1'=1, 'dc2'=1, 'dc3'=1, 'dc4'=1}",
			Ring: scyllaclient.Ring{
				ReplicaTokens: []scyllaclient.ReplicaTokenRanges{
					rs("h1", "h2", "h3", "h6"), rs("h1", "h2", "h4", "h6"), rs("h1", "h2", "h5", "h6"),
					rs("h1", "h2", "h3", "h7"), rs("h1", "h2", "h4", "h7"), rs("h1", "h2", "h5", "h7"),
					rs("h1", "h2", "h3", "h8"), rs("h1", "h2", "h4", "h8"), rs("h1", "h2", "h5", "h8"),
					rs("h1", "h2", "h3", "h9"), rs("h1", "h2", "h4", "h9"), rs("h1", "h2", "h5", "h9"),
				},
				HostDC: hostDC,
			},
			DCs:      []string{"dc1"},
			Expected: false, // rs("h1", "h2", "h3", "h4") has 1 replica in dc1
		},
		{
			Name: "--dc 'dc3', NetworkTopologyStrategy{'dc1'=1, 'dc2'=1, 'dc3'=2, 'dc4'=2}",
			Ring: scyllaclient.Ring{
				ReplicaTokens: []scyllaclient.ReplicaTokenRanges{
					rs("h1", "h2", "h3", "h4", "h6", "h7"), rs("h1", "h2", "h5", "h4", "h6", "h7"), rs("h1", "h2", "h3", "h5", "h6", "h7"),
					rs("h1", "h2", "h3", "h4", "h6", "h8"), rs("h1", "h2", "h5", "h4", "h6", "h8"), rs("h1", "h2", "h3", "h5", "h6", "h8"),
					rs("h1", "h2", "h3", "h4", "h6", "h9"), rs("h1", "h2", "h5", "h4", "h6", "h9"), rs("h1", "h2", "h3", "h5", "h6", "h9"),
					rs("h1", "h2", "h3", "h4", "h7", "h8"), rs("h1", "h2", "h5", "h4", "h7", "h8"), rs("h1", "h2", "h3", "h5", "h7", "h8"),
					rs("h1", "h2", "h3", "h4", "h8", "h9"), rs("h1", "h2", "h5", "h4", "h8", "h9"), rs("h1", "h2", "h3", "h5", "h8", "h9"),
				},
				HostDC: hostDC,
			},
			DCs:      []string{"dc3"},
			Expected: true,
		},
		{
			Name: "--dc 'dc4', --host h6, NetworkTopologyStrategy{'dc1'=1, 'dc2'=1, 'dc3'=3, 'dc4'=4}",
			Ring: scyllaclient.Ring{
				ReplicaTokens: []scyllaclient.ReplicaTokenRanges{
					rs("h1", "h2", "h3", "h4", "h5", "h6", "h7", "h8", "h9"),
				},
				HostDC: hostDC,
			},
			DCs:      []string{"dc4"},
			Host:     "h6",
			Expected: true,
		},
		{
			Name: "--dc 'dc4', --host h2, NetworkTopologyStrategy{'dc1'=1, 'dc2'=1, 'dc3'=3, 'dc4'=4}",
			Ring: scyllaclient.Ring{
				ReplicaTokens: []scyllaclient.ReplicaTokenRanges{
					rs("h1", "h2", "h3", "h4", "h5", "h6", "h7", "h8", "h9"),
				},
				HostDC: hostDC,
			},
			DCs:      []string{"dc4"},
			Host:     "h2",
			Expected: false, // h2 is not in dc4
		},
		{
			Name: "--host h1, NetworkTopologyStrategy{'dc1'=1, 'dc2'=1, 'dc3'=3, 'dc4'=4}",
			Ring: scyllaclient.Ring{
				ReplicaTokens: []scyllaclient.ReplicaTokenRanges{
					rs("h1", "h2", "h3", "h4", "h5", "h6", "h7", "h8", "h9"),
				},
				HostDC: hostDC,
			},
			DCs:      allDCs,
			Host:     "h1",
			Expected: true,
		},
	}

	for i := range testCases {
		tc := testCases[i]
		t.Run(tc.Name, func(t *testing.T) {
			t.Parallel()
			if out := repair.ShouldRepairRing(tc.Ring, tc.DCs, hostIP(tc.Host)); out != tc.Expected {
				t.Fatalf("Expected %v, got %v", tc.Expected, out)
			}
		})
	}
}

// dummyHost is a number (possibly prefixed with "h").
func hostIP(dummyHost string) netip.Addr {
	if dummyHost == "" {
		return netip.Addr{}
	}
	return netip.MustParseAddr("192.168.100." + strings.TrimPrefix(dummyHost, "h"))
}
