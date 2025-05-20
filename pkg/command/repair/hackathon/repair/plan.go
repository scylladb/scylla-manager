// Copyright (C) 2023 ScyllaDB

package repair

import (
	"context"
	"math"
	"net/netip"
	"slices"
	"sort"
	"sync/atomic"

	"github.com/pkg/errors"
	"github.com/scylladb/go-set/strset"
	"github.com/scylladb/scylla-manager/v3/pkg/scyllaclient"
	"github.com/scylladb/scylla-manager/v3/pkg/util/slice"
	"golang.org/x/sync/errgroup"
)

// plan describes whole repair schedule and state.
type plan struct {
	Keyspaces keyspacePlans

	Hosts            []string
	MaxParallel      int
	MaxHostIntensity map[netip.Addr]Intensity
	apiSupport       apiSupport
	// Used for progress purposes
	Stats map[scyllaclient.HostKeyspaceTable]tableStats
}

type keyspacePlans []keyspacePlan

// keyspacePlan describes repair schedule and state for keyspace.
type keyspacePlan struct {
	Keyspace string
	Size     int64
	Tablet   bool
	Tables   []tablePlan
}

// tablePlan describes repair schedule and state for table.
type tablePlan struct {
	Table         string
	Size          int64
	RangesCnt     int
	ReplicaSetCnt int
	Small         bool
}

type tableStats struct {
	Size   int64
	Ranges int
}

func newPlan(ctx context.Context, target Target, client *scyllaclient.Client) (*plan, error) {
	status, err := client.Status(ctx)
	if err != nil {
		return nil, errors.Wrap(err, "get status")
	}

	var (
		ks       keyspacePlans
		ranges   = make(map[scyllaclient.HostKeyspaceTable]int)
		allHosts = strset.New()
		maxP     int
	)

	ringDescriber := scyllaclient.NewRingDescriber(ctx, client)
	for _, u := range target.Units {
		var tables []tablePlan
		for _, t := range u.Tables {
			ring, err := ringDescriber.DescribeRing(ctx, u.Keyspace, t)
			if err != nil {
				return nil, errors.Wrapf(err, "keyspace %s.%s: get ring description", u.Keyspace, t)
			}
			// Allow repairing single node cluster for better UX.
			if len(status) > 1 && !ShouldRepairRing(ring, target.DC, target.Host) {
				continue
			}

			// Update max parallel
			maxP = max(maxP, MaxRingParallel(ring, target.DC))

			// Update ranges and hosts
			rangesCnt := 0
			replicaSetCnt := 0
			for _, rep := range ring.ReplicaTokens {
				filtered := filterReplicaSet(rep.ReplicaSet, ring.HostDC, target)
				if len(filtered) == 0 {
					continue
				}

				replicaSetCnt++
				for _, h := range filtered {
					allHosts.Add(h.String())
					ranges[newHostKsTable(h.String(), u.Keyspace, t)] += len(rep.Ranges)
				}
				rangesCnt += len(rep.Ranges)
			}

			tables = append(tables, tablePlan{
				Table:         t,
				ReplicaSetCnt: replicaSetCnt,
				RangesCnt:     rangesCnt,
			})
		}

		if len(tables) > 0 {
			ks = append(ks, keyspacePlan{
				Keyspace: u.Keyspace,
				Tablet:   ringDescriber.IsTabletKeyspace(u.Keyspace),
				Tables:   tables,
			})
		}
	}

	if len(ks) == 0 {
		return nil, ErrEmptyRepair
	}

	// Update size and optimize
	hosts := allHosts.List()
	sizeReport, err := ks.fillSize(ctx, client, hosts)
	if err != nil {
		return nil, err
	}
	ks.fillSmall(target.SmallTableThreshold)

	support, err := getRepairAPISupport(ctx, client, hosts)
	if err != nil {
		return nil, errors.Wrap(err, "check support of small_table_optimization")
	}

	// Update max host intensity
	mhi, err := maxHostIntensity(ctx, client, hosts)
	if err != nil {
		return nil, errors.Wrap(err, "calculate max host intensity")
	}

	return &plan{
		Keyspaces:        ks,
		Hosts:            hosts,
		MaxParallel:      maxP,
		MaxHostIntensity: mhi,
		apiSupport:       support,
		Stats:            newStats(sizeReport, ranges),
	}, nil
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

func (p keyspacePlans) fillSize(ctx context.Context, client *scyllaclient.Client, hosts []string) ([]scyllaclient.SizeReport, error) {
	var hkts []scyllaclient.HostKeyspaceTable
	for _, ksp := range p {
		for _, tp := range ksp.Tables {
			for _, h := range hosts {
				hkts = append(hkts, newHostKsTable(h, ksp.Keyspace, tp.Table))
			}
		}
	}

	sizeReport, err := client.TableDiskSizeReport(ctx, hkts)
	if err != nil {
		return nil, errors.Wrap(err, "calculate tables size")
	}

	ksSize := make(map[string]int64)
	tableSize := make(map[string]int64)
	for _, sr := range sizeReport {
		ksSize[sr.Keyspace] += sr.Size
		tableSize[sr.Keyspace+"."+sr.Table] += sr.Size
	}

	for i, ksp := range p {
		p[i].Size = ksSize[ksp.Keyspace]
		for j, tp := range ksp.Tables {
			ksp.Tables[j].Size = tableSize[ksp.Keyspace+"."+tp.Table]
		}
	}
	return sizeReport, nil
}

func (p keyspacePlans) fillSmall(smallTableThreshold int64) {
	for _, ksp := range p {
		for j, tp := range ksp.Tables {
			if tp.Size < smallTableThreshold {
				ksp.Tables[j].Small = true
			}
		}
	}
}

// ShouldRepairRing when all ranges are replicated (len(replicaSet) > 1) in specified dcs.
// If host is set, it also checks if host belongs to the dcs.
func ShouldRepairRing(ring scyllaclient.Ring, dcs []string, host netip.Addr) bool {
	repairedDCs := strset.New(dcs...)
	if host.IsValid() {
		if dc, ok := ring.HostDC[host]; !ok || !repairedDCs.Has(dc) {
			return false
		}
	}

	for _, rt := range ring.ReplicaTokens {
		rep := 0
		for _, r := range rt.ReplicaSet {
			if repairedDCs.Has(ring.HostDC[r]) {
				rep++
			}
		}
		if rep <= 1 {
			return false
		}
	}
	return true
}

// maxHostIntensity sets max_ranges_in_parallel for all repaired host.
func maxHostIntensity(ctx context.Context, client *scyllaclient.Client, hosts []string) (map[netip.Addr]Intensity, error) {
	shards, err := client.HostsShardCount(ctx, hosts)
	if err != nil {
		return nil, err
	}
	memory, err := client.HostsTotalMemory(ctx, hosts)
	if err != nil {
		return nil, err
	}
	return hostMaxRanges(shards, memory)
}

func hostMaxRanges(shards map[string]uint, memory map[string]int64) (map[netip.Addr]Intensity, error) {
	out := make(map[netip.Addr]Intensity, len(shards))
	for h, sh := range shards {
		ip, err := netip.ParseAddr(h)
		if err != nil {
			return nil, err
		}
		out[ip] = maxRepairRangesInParallel(sh, memory[h])
	}
	return out, nil
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

// MaxRingParallel calculates max amount of repair jobs on ring limited to dcs.
func MaxRingParallel(ring scyllaclient.Ring, dcs []string) int {
	repairedDCs := strset.New(dcs...)
	dcNodeCnt := make(map[string]int)
	for _, dc := range ring.HostDC {
		dcNodeCnt[dc]++
	}

	switch ring.Replication {
	case scyllaclient.SimpleStrategy:
		repaired := 0
		for dc, cnt := range dcNodeCnt {
			if repairedDCs.Has(dc) {
				repaired += cnt
			}
		}
		return repaired / ring.RF
	case scyllaclient.NetworkTopologyStrategy:
		minDC := math.MaxInt / 2
		for dc, rf := range ring.DCrf {
			if repairedDCs.Has(dc) {
				minDC = min(minDC, dcNodeCnt[dc]/rf)
			}
		}
		if minDC == math.MaxInt/2 {
			minDC = 1
		}
		return minDC
	default:
		return 1
	}
}

// Filters replica set according to --dc, --ignore-down-hosts, --host.
func filterReplicaSet(replicaSet []netip.Addr, hostDC map[netip.Addr]string, target Target) []netip.Addr {
	if target.Host.IsValid() && !slices.Contains(replicaSet, target.Host) {
		return nil
	}
	var out []netip.Addr
	for _, h := range replicaSet {
		if slice.ContainsString(target.DC, hostDC[h]) && !slices.Contains(target.IgnoreHosts, h) {
			out = append(out, h)
		}
	}
	return out
}

func newStats(sizeReport []scyllaclient.SizeReport, ranges map[scyllaclient.HostKeyspaceTable]int) map[scyllaclient.HostKeyspaceTable]tableStats {
	out := make(map[scyllaclient.HostKeyspaceTable]tableStats, len(sizeReport))
	for _, sr := range sizeReport {
		out[sr.HostKeyspaceTable] = tableStats{
			Size:   sr.Size,
			Ranges: ranges[sr.HostKeyspaceTable],
		}
	}
	return out
}

func newHostKsTable(host, ks, table string) scyllaclient.HostKeyspaceTable {
	return scyllaclient.HostKeyspaceTable{
		Host:     host,
		Keyspace: ks,
		Table:    table,
	}
}

// apiSupport describes the support for optimized repair API
// calls which are not present in all supported Scylla versions.
type apiSupport struct {
	// If /storage_service/repair_async/{keyspace} API supports
	// 'small_table_optimization' query param.
	smallTableRepair bool
	// If /storage_service/tablets/repair API is exposed.
	tabletRepair bool
}

func getRepairAPISupport(ctx context.Context, client *scyllaclient.Client, hosts []string) (apiSupport, error) {
	smallTableOpt := atomic.Bool{}
	smallTableOpt.Store(true)
	fullTabletTableOpt := atomic.Bool{}
	fullTabletTableOpt.Store(true)
	eg := errgroup.Group{}

	for _, host := range hosts {
		h := host
		eg.Go(func() error {
			ni, err := client.NodeInfo(ctx, h)
			if err != nil {
				return err
			}

			res, err := ni.SupportsRepairSmallTableOptimization()
			if err != nil {
				return err
			}
			if !res {
				smallTableOpt.Store(false)
			}

			res, err = ni.SupportsTabletRepair()
			if err != nil {
				return err
			}
			if !res {
				fullTabletTableOpt.Store(false)
			}

			return nil
		})
	}

	if err := eg.Wait(); err != nil {
		return apiSupport{}, err
	}
	return apiSupport{
		smallTableRepair: smallTableOpt.Load(),
		tabletRepair:     fullTabletTableOpt.Load(),
	}, nil
}
