// Copyright (C) 2023 ScyllaDB

package repair

import (
	"context"
	"math"
	"sort"

	"github.com/pkg/errors"
	"github.com/scylladb/go-set/strset"
	"github.com/scylladb/scylla-manager/v3/pkg/scyllaclient"
	"github.com/scylladb/scylla-manager/v3/pkg/util/slice"
)

type hkt = scyllaclient.HostKeyspaceTable

// plan describes whole repair schedule and state.
type plan struct {
	Keyspaces []keyspacePlan

	Hosts            []string
	MaxParallel      int
	MaxHostIntensity map[string]Intensity
	// Used for progress purposes
	HostSize   map[hkt]int64
	HostRanges map[hkt]int
}

func newPlan(ctx context.Context, target Target, client *scyllaclient.Client) (*plan, error) {
	status, err := client.Status(ctx)
	if err != nil {
		return nil, errors.Wrap(err, "get status")
	}

	dcMap, err := client.Datacenters(ctx)
	if err != nil {
		return nil, errors.Wrap(err, "get datacenters")
	}

	var (
		keyspaces  []keyspacePlan
		hostRanges = make(map[hkt]int)
		allHosts   = strset.New()
		maxP       int
	)

	ringDescriber := scyllaclient.NewRingDescriber(ctx, client)
	for _, u := range target.Units {
		var tables []tablePlan
		for _, t := range u.Tables {
			ring, err := ringDescriber.DescribeRing(ctx, u.Keyspace, t)
			if err != nil {
				return nil, errors.Wrapf(err, "get ring description of %s.%s", u.Keyspace, t)
			}
			if !filteredRing(target, status, ring) {
				continue
			}

			var (
				sets      [][]string
				rangesCnt int
			)
			for _, rep := range ring.ReplicaTokens {
				for _, h := range rep.ReplicaSet {
					k := hkt{
						Host:     h,
						Keyspace: u.Keyspace,
						Table:    t,
					}
					hostRanges[k] += len(rep.Ranges)
				}
				sets = append(sets, rep.ReplicaSet)
				allHosts.Add(rep.ReplicaSet...)
				rangesCnt += len(rep.Ranges)
			}

			if p := maxParallel(dcMap, sets); maxP < p {
				maxP = p
			}
			tables = append(tables, tablePlan{
				Table:         t,
				Size:          -1, // Filled with fillSize
				ReplicaSetCnt: len(sets),
				RangesCnt:     rangesCnt,
				Optimize:      false, // Filled with fillOptimize
			})
		}
		keyspaces = append(keyspaces, keyspacePlan{
			Keyspace: u.Keyspace,
			Size:     -1, // Filled with fillSize
			Tables:   tables,
		})
	}

	if len(keyspaces) == 0 {
		return nil, ErrEmptyRepair
	}
	hosts := allHosts.List()

	var hkts []hkt
	for _, ksp := range keyspaces {
		for _, tp := range ksp.Tables {
			for _, h := range hosts {
				hkts = append(hkts, hkt{Host: h, Keyspace: ksp.Keyspace, Table: tp.Table})
			}
		}
	}
	hostSize, err := perHostTableSize(ctx, client, hkts)
	if err != nil {
		return nil, errors.Wrap(err, "calculate tables size")
	}
	fillSize(hostSize, keyspaces)
	fillOptimize(keyspaces, target.SmallTableThreshold)

	mhi, err := maxHostIntensity(ctx, client, hosts)
	if err != nil {
		return nil, errors.Wrap(err, "calculate max host intensity")
	}

	return &plan{
		Keyspaces:        keyspaces,
		Hosts:            hosts,
		MaxParallel:      maxP,
		MaxHostIntensity: mhi,
		HostSize:         hostSize,
		HostRanges:       hostRanges,
	}, nil
}

// FilteredUnits returns repaired tables in unit format.
func (p *plan) FilteredUnits(units []Unit) []Unit {
	allTables := make(map[string]int)
	for _, u := range units {
		if u.AllTables {
			allTables[u.Keyspace] = len(u.Tables)
		} else {
			allTables[u.Keyspace] = -1
		}
	}
	var out []Unit
	for _, kp := range p.Keyspaces {
		u := Unit{
			Keyspace:  kp.Keyspace,
			AllTables: allTables[kp.Keyspace] == len(kp.Tables),
		}
		for _, tp := range kp.Tables {
			u.Tables = append(u.Tables, tp.Table)
		}
		out = append(out, u)
	}
	return out
}

func perHostTableSize(ctx context.Context, client *scyllaclient.Client, hkts []hkt) (map[hkt]int64, error) {
	report, err := client.TableDiskSizeReport(ctx, hkts)
	if err != nil {
		return nil, errors.Wrap(err, "fetch table disk size report")
	}
	hostSize := make(map[hkt]int64, len(hkts))
	for i, size := range report {
		hostSize[hkt{
			Host:     hkts[i].Host,
			Keyspace: hkts[i].Keyspace,
			Table:    hkts[i].Table,
		}] = size
	}
	return hostSize, nil
}

func fillSize(hostSize map[hkt]int64, keyspaces []keyspacePlan) {
	ksSize := make(map[string]int64)
	tableSize := make(map[string]int64)
	for k, v := range hostSize {
		ksSize[k.Keyspace] += v
		tableSize[k.Keyspace+"."+k.Table] += v
	}
	for i, ksp := range keyspaces {
		keyspaces[i].Size = ksSize[ksp.Keyspace]
		for j, tp := range ksp.Tables {
			ksp.Tables[j].Size = tableSize[ksp.Keyspace+"."+tp.Table]
		}
	}
}

func fillOptimize(keyspaces []keyspacePlan, smallTableThreshold int64) {
	for _, ksp := range keyspaces {
		for j, tp := range ksp.Tables {
			// Return merged ranges for small, fully replicated table (#3128)
			if tp.Size < smallTableThreshold && tp.ReplicaSetCnt == 1 {
				ksp.Tables[j].Optimize = true
			}
		}
	}
}

// maxHostIntensity sets max_ranges_in_parallel for all repaired host.
func maxHostIntensity(ctx context.Context, client *scyllaclient.Client, hosts []string) (map[string]Intensity, error) {
	shards, err := client.HostsShardCount(ctx, hosts)
	if err != nil {
		return nil, err
	}
	memory, err := client.HostsTotalMemory(ctx, hosts)
	if err != nil {
		return nil, err
	}
	return hostMaxRanges(shards, memory), nil
}

func hostMaxRanges(shards map[string]uint, memory map[string]int64) map[string]Intensity {
	out := make(map[string]Intensity, len(shards))
	for h, sh := range shards {
		out[h] = maxRepairRangesInParallel(sh, memory[h])
	}
	return out
}

func maxRepairRangesInParallel(shards uint, totalMemory int64) Intensity {
	const MiB = 1024 * 1024
	memoryPerShard := totalMemory / int64(shards)
	maxI := int(0.1 * float64(memoryPerShard) / (32 * MiB) / 4)
	if maxI == 0 {
		maxI = 1
	}
	return NewIntensity(maxI)
}

// maxParallel returns maximal repair parallelism limited to given replica sets.
func maxParallel(dcMap map[string][]string, sets [][]string) int {
	minP := math.MaxInt
	for _, dcHosts := range dcMap {
		// Max keyspace parallel is equal to the smallest max DC parallel
		if p := maxDCParallel(dcHosts, sets); p < minP {
			minP = p
		}
	}
	return minP
}

// maxDCParallel returns maximal repair parallelism limited to given replica sets and nodes of given dc.
func maxDCParallel(dcHosts []string, sets [][]string) int {
	var maxP int
	dcHostsSet := strset.New(dcHosts...)

	for _, rep := range sets {
		filteredRepSet := setIntersection(strset.New(rep...), dcHostsSet)
		if filteredRepSet.Size() == 0 {
			continue
		}
		// Max DC parallel is equal to #(repaired nodes from DC) / #(smallest partial replica set from DC)
		if p := dcHostsSet.Size() / filteredRepSet.Size(); maxP < p {
			maxP = p
		}
	}

	if maxP == 0 {
		return math.MaxInt
	}
	return maxP
}

// ViewSort ensures that views are repaired after base tables.
func (p *plan) ViewSort(views *strset.Set) {
	for _, kp := range p.Keyspaces {
		sort.SliceStable(kp.Tables, func(i, j int) bool {
			kst1 := kp.Keyspace + "." + kp.Tables[i].Table
			kst2 := kp.Keyspace + "." + kp.Tables[j].Table
			return !views.Has(kst1) && views.Has(kst2)
		})
	}
}

// PrioritySort ensures that table with priority are repaired first.
func (p *plan) PrioritySort(pref TablePreference) {
	sort.SliceStable(p.Keyspaces, func(i, j int) bool {
		return pref.KSLess(p.Keyspaces[i].Keyspace, p.Keyspaces[j].Keyspace)
	})
	for _, kp := range p.Keyspaces {
		sort.SliceStable(kp.Tables, func(i, j int) bool {
			return pref.TLess(kp.Keyspace, kp.Tables[i].Table, kp.Tables[j].Table)
		})
	}
}

// SizeSort ensures that smaller tables are repaired first.
func (p *plan) SizeSort() {
	sort.SliceStable(p.Keyspaces, func(i, j int) bool {
		return p.Keyspaces[i].Size < p.Keyspaces[j].Size
	})
	for _, kp := range p.Keyspaces {
		sort.SliceStable(kp.Tables, func(i, j int) bool {
			return kp.Tables[i].Size < kp.Tables[j].Size
		})
	}
}

// keyspacePlan describes repair schedule and state for keyspace.
type keyspacePlan struct {
	Keyspace string
	Size     int64
	Tables   []tablePlan
}

func setIntersection(s1, s2 *strset.Set) *strset.Set {
	out := strset.New()
	if s2.Size() < s1.Size() {
		s1, s2 = s2, s1
	}
	s1.Each(func(item string) bool {
		if s2.Has(item) {
			out.Add(item)
		}
		return true
	})
	return out
}

// tablePlan describes repair schedule and state for table.
type tablePlan struct {
	Table         string
	Size          int64
	RangesCnt     int
	ReplicaSetCnt int
	// Optimized tables (small and fully replicated)
	// have all ranges for replica set repaired in a single job.
	Optimize bool
}

func filteredRing(target Target, status scyllaclient.NodeStatusInfoSlice, ring scyllaclient.Ring) bool {
	// Allow repairing single node cluster for better UX and tests
	if ring.Replication == scyllaclient.LocalStrategy && len(status) > 1 {
		return false
	}

	filtered := filteredHosts(target, status)
	for i, rep := range ring.ReplicaTokens {
		// Don't add keyspace with some ranges not replicated in filtered hosts,
		// unless it's a single node cluster.
		rs := filteredReplicaSet(rep.ReplicaSet, filtered, target.Host)
		if len(rs) <= 1 && len(status) > 1 {
			return false
		}
		ring.ReplicaTokens[i].ReplicaSet = rs
	}
	return true
}

// filteredHosts returns hosts passing '--dc' and '--ignore-down-hosts' criteria.
func filteredHosts(target Target, status scyllaclient.NodeStatusInfoSlice) *strset.Set {
	ignoredHosts := strset.New(target.IgnoreHosts...)
	dcs := strset.New(target.DC...)
	filtered := strset.New()

	for _, node := range status {
		if !ignoredHosts.Has(node.Addr) && dcs.Has(node.Datacenter) {
			filtered.Add(node.Addr)
		}
	}

	return filtered
}

// filterReplicaSet returns hosts present in filteredHosts and passing '--host' criteria.
func filteredReplicaSet(replicaSet []string, filteredHosts *strset.Set, host string) []string {
	var out []string
	for _, r := range replicaSet {
		if filteredHosts.Has(r) {
			out = append(out, r)
		}
	}

	if host != "" && !slice.ContainsString(out, host) {
		out = nil
	}

	return out
}
