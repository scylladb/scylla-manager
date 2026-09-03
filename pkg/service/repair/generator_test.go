// Copyright (C) 2026 ScyllaDB

package repair

import (
	"cmp"
	"net/netip"
	"slices"
	"testing"

	"github.com/scylladb/scylla-manager/v3/pkg/scyllaclient"
)

// TestRepairFilters verifies that the dc/host filtering params
// produced by repairFilters are:
// - correct - they result in expected replica set
// - are not a no-op - they are not set when not needed
func TestRepairFilters(t *testing.T) {
	var (
		h1 = netip.MustParseAddr("192.168.100.11") // dc1
		h2 = netip.MustParseAddr("192.168.100.12") // dc1
		h3 = netip.MustParseAddr("192.168.100.21") // dc2
		h4 = netip.MustParseAddr("192.168.100.22") // dc2
	)
	ring := scyllaclient.Ring{
		HostDC: map[netip.Addr]string{h1: "dc1", h2: "dc1", h3: "dc2", h4: "dc2"},
	}

	testCases := []struct {
		name       string
		target     Target
		jobType    jobType
		repSet     []netip.Addr
		hostFilter []netip.Addr
		dcFilter   []string
	}{
		{
			name:    "tablet repair without filtering has no filtering params",
			target:  Target{DC: []string{"dc1", "dc2"}},
			jobType: tabletJobType,
			repSet:  []netip.Addr{h1, h2, h3, h4},
		},
		{
			name:     "tablet repair sets effective dc filter",
			target:   Target{DC: []string{"dc1"}},
			jobType:  tabletJobType,
			repSet:   []netip.Addr{h1, h2},
			dcFilter: []string{"dc1"},
		},
		{
			name:       "small table repair sets effective host filter",
			target:     Target{DC: []string{"dc1", "dc2"}, IgnoreHosts: []netip.Addr{h4}},
			jobType:    smallTableJobType,
			repSet:     []netip.Addr{h1, h2, h3},
			hostFilter: []netip.Addr{h1, h2, h3},
		},
		{
			name:       "tablet repair combines dc and host filters",
			target:     Target{DC: []string{"dc2"}, IgnoreHosts: []netip.Addr{h4}},
			jobType:    tabletJobType,
			repSet:     []netip.Addr{h3},
			hostFilter: []netip.Addr{h1, h2, h3},
			dcFilter:   []string{"dc2"},
		},
		{
			name:       "small table repair keeps no-op host filter",
			target:     Target{DC: []string{"dc1", "dc2"}},
			jobType:    smallTableJobType,
			repSet:     []netip.Addr{h1, h2, h3, h4},
			hostFilter: []netip.Addr{h1, h2, h3, h4},
		},
		{
			name:       "small table repair encodes dc filter into host filter",
			target:     Target{DC: []string{"dc1"}},
			jobType:    smallTableJobType,
			repSet:     []netip.Addr{h1, h2},
			hostFilter: []netip.Addr{h1, h2},
		},
		{
			name:       "small table repair encodes combined dc and host filters into host filter",
			target:     Target{DC: []string{"dc2"}, IgnoreHosts: []netip.Addr{h4}},
			jobType:    smallTableJobType,
			repSet:     []netip.Addr{h3},
			hostFilter: []netip.Addr{h3},
		},
		{
			name:       "not full table repair filters by its replica set",
			target:     Target{DC: []string{"dc1"}, IgnoreHosts: []netip.Addr{h2}},
			jobType:    normalJobType,
			repSet:     []netip.Addr{h1},
			hostFilter: []netip.Addr{h1},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			tg := &tableGenerator{
				generatorTools: generatorTools{target: tc.target},
				Ring:           ring,
			}

			dcFilter, hostFilter := tg.repairFilters(tc.jobType, tc.repSet)
			if !equalAddrs(hostFilter, tc.hostFilter) {
				t.Errorf("got host filter %v, expected %v", hostFilter, tc.hostFilter)
			}
			if !equalSlices(dcFilter, tc.dcFilter) {
				t.Errorf("got dc filter %v, expected %v", dcFilter, tc.dcFilter)
			}
		})
	}
}

func equalAddrs(a, b []netip.Addr) bool {
	f := func(x, y netip.Addr) int { return x.Compare(y) }
	return slices.Equal(slices.SortedFunc(slices.Values(a), f), slices.SortedFunc(slices.Values(b), f))
}

func equalSlices[T cmp.Ordered](a, b []T) bool {
	return slices.Equal(slices.Sorted(slices.Values(a)), slices.Sorted(slices.Values(b)))
}
