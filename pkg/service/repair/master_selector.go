// Copyright (C) 2024 ScyllaDB

package repair

import (
	"maps"
	"math"
	"net/netip"
	"slices"
	"sort"

	"github.com/scylladb/scylla-manager/v3/pkg/util/slice"
)

// masterSelector describes each host priority for being repair master.
// Repair master is first chosen by smallest shard count,
// then by smallest dc RTT from SM.
type masterSelector map[netip.Addr]int

func newMasterSelector(shards map[netip.Addr]uint, hostDC map[netip.Addr]string, closestDC []string) masterSelector {
	hosts := slices.Collect(maps.Keys(shards))
	sort.Slice(hosts, func(i, j int) bool {
		if shards[hosts[i]] != shards[hosts[j]] {
			return shards[hosts[i]] < shards[hosts[j]]
		}
		return slice.Index(closestDC, hostDC[hosts[i]]) < slice.Index(closestDC, hostDC[hosts[j]])
	})

	ms := make(masterSelector)
	for i, h := range hosts {
		ms[h] = i
	}
	return ms
}

// Select returns repair master from replica set.
func (ms masterSelector) Select(replicas []netip.Addr) netip.Addr {
	var master netip.Addr
	p := math.MaxInt64
	for _, r := range replicas {
		if ms[r] < p {
			p = ms[r]
			master = r
		}
	}
	return master
}
