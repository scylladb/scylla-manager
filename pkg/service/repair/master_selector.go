// Copyright (C) 2024 ScyllaDB

package repair

import (
	"maps"
	"math"
	"net/netip"
	"slices"
	"sort"

	"github.com/scylladb/scylla-manager/v3/pkg/util/slice"
	"github.com/scylladb/scylla-manager/v3/pkg/util2"
)

// masterSelector describes each host priority for being repair master.
// Repair master is first chosen by smallest shard count,
// then by smallest dc RTT from SM.
type masterSelector map[netip.Addr]int

func newMasterSelector(shards map[string]uint, hostDC map[string]string, closestDC []string) (masterSelector, error) {
	hosts, err := util2.ConvertSliceWithError(slices.Collect(maps.Keys(shards)), netip.ParseAddr)
	if err != nil {
		return nil, err
	}

	sort.Slice(hosts, func(i, j int) bool {
		if shards[hosts[i].String()] != shards[hosts[j].String()] {
			return shards[hosts[i].String()] < shards[hosts[j].String()]
		}
		return slice.Index(closestDC, hostDC[hosts[i].String()]) < slice.Index(closestDC, hostDC[hosts[j].String()])
	})

	ms := make(masterSelector)
	for i, h := range hosts {
		ms[h] = i
	}
	return ms, nil
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
