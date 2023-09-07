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

// plan describes whole repair schedule and state.
type plan struct {
	Keyspaces        []keyspacePlan
	Idx              int // Idx of currently repaired keyspace
	SkippedKeyspaces []string
}

func newPlan(ctx context.Context, target Target, client *scyllaclient.Client) (*plan, error) {
	filtered, err := filteredHosts(ctx, target, client)
	if err != nil {
		return nil, errors.Wrap(err, "filter hosts")
	}

	p := new(plan)
	for _, u := range target.Units {
		ring, err := client.DescribeRing(ctx, u.Keyspace)
		if err != nil {
			return nil, errors.Wrapf(err, "keyspace %s: get ring description", u.Keyspace)
		}
		if ring.Replication == scyllaclient.LocalStrategy {
			continue
		}

		kp := keyspacePlan{
			Keyspace:    u.Keyspace,
			TokenRepIdx: make(map[scyllaclient.TokenRange]int),
			AllTables:   u.AllTables,
		}

		skip := false
		for _, rep := range ring.ReplicaTokens {
			rtr := scyllaclient.ReplicaTokenRanges{
				ReplicaSet: filteredReplicaSet(rep.ReplicaSet, filtered, target.Host),
				Ranges:     rep.Ranges,
			}

			// Don't add keyspace with some ranges not replicated in filtered hosts
			if len(rtr.ReplicaSet) <= 1 {
				skip = true
				break
			}

			for _, r := range rtr.Ranges {
				kp.TokenRepIdx[r] = len(kp.Replicas)
			}
			kp.Replicas = append(kp.Replicas, rtr)
		}

		if skip {
			p.SkippedKeyspaces = append(p.SkippedKeyspaces, u.Keyspace)
			continue
		}

		// Fill tables
		for _, t := range u.Tables {
			kp.Tables = append(kp.Tables, tablePlan{
				Table:           t,
				MarkedRanges:    make(map[scyllaclient.TokenRange]struct{}),
				MarkedInReplica: make([]int, len(kp.Replicas)),
			})
		}
		p.Keyspaces = append(p.Keyspaces, kp)
	}

	if len(p.Keyspaces) == 0 {
		return nil, ErrEmptyRepair
	}
	if err := p.FillSize(ctx, client, target.SmallTableThreshold); err != nil {
		return nil, errors.Wrap(err, "calculate tables size")
	}
	return p, nil
}

// UpdateIdx sets keyspace and table idx to the next not repaired table.
// Returns false if there are no more tables to repair.
func (p *plan) UpdateIdx() bool {
	ksIdx := p.Idx
	tabIdx := p.Keyspaces[ksIdx].Idx

	for ; ksIdx < len(p.Keyspaces); ksIdx++ {
		kp := p.Keyspaces[ksIdx]
		for ; tabIdx < len(kp.Tables); tabIdx++ {
			// Always wait for current table to be fully repaired before moving to the next one
			if !kp.IsTableRepaired(tabIdx) {
				p.Idx = ksIdx
				p.Keyspaces[ksIdx].Idx = tabIdx
				return true
			}
		}
		tabIdx = 0
	}

	return false
}

// Hosts returns all hosts taking part in repair.
func (p *plan) Hosts() []string {
	out := strset.New()
	for _, kp := range p.Keyspaces {
		for _, rep := range kp.Replicas {
			out.Add(rep.ReplicaSet...)
		}
	}
	return out.List()
}

// Units returns repaired tables in unit format.
func (p *plan) Units() []Unit {
	var out []Unit
	for _, kp := range p.Keyspaces {
		u := Unit{
			Keyspace:  kp.Keyspace,
			AllTables: kp.AllTables,
		}
		for _, tp := range kp.Tables {
			u.Tables = append(u.Tables, tp.Table)
		}
		out = append(out, u)
	}
	return out
}

// MinRF returns the smallest repaired replica set size.
func (p *plan) MinRF() int {
	min := math.MaxInt64
	for _, kp := range p.Keyspaces {
		for _, rep := range kp.Replicas {
			if min > len(rep.ReplicaSet) {
				min = len(rep.ReplicaSet)
			}
		}
	}
	return min
}

func (p *plan) MarkDeleted(keyspace, table string) {
	for _, kp := range p.Keyspaces {
		if kp.Keyspace != keyspace {
			continue
		}
		for tabIdx, tp := range kp.Tables {
			if tp.Table == table {
				kp.Tables[tabIdx].Deleted = true
				return
			}
		}
	}
}

func (p *plan) MarkDoneRanges(keyspace, table string, cnt int) {
	for _, kp := range p.Keyspaces {
		if kp.Keyspace != keyspace {
			continue
		}
		for tabIdx, tp := range kp.Tables {
			if tp.Table == table {
				kp.Tables[tabIdx].Done += cnt
				return
			}
		}
	}
}

// FillSize sets size and optimize of each table.
func (p *plan) FillSize(ctx context.Context, client *scyllaclient.Client, smallTableThreshold int64) error {
	var hkts []scyllaclient.HostKeyspaceTable
	hosts := p.Hosts()
	for _, kp := range p.Keyspaces {
		for _, tp := range kp.Tables {
			for _, h := range hosts {
				hkts = append(hkts, scyllaclient.HostKeyspaceTable{Host: h, Keyspace: kp.Keyspace, Table: tp.Table})
			}
		}
	}

	report, err := client.TableDiskSizeReport(ctx, hkts)
	if err != nil {
		return errors.Wrap(err, "fetch table disk size report")
	}

	ksSize := make(map[string]int64)
	tableSize := make(map[string]int64)
	for i, size := range report {
		ksSize[hkts[i].Keyspace] += size
		tableSize[hkts[i].Keyspace+"."+hkts[i].Table] += size
	}

	for i, kp := range p.Keyspaces {
		p.Keyspaces[i].Size = ksSize[kp.Keyspace]
		for j, tp := range kp.Tables {
			kp.Tables[j].Size = tableSize[kp.Keyspace+"."+tp.Table]
			// Return merged ranges for small, fully replicated table (#3128)
			if tp.Size <= smallTableThreshold && len(kp.Replicas) == 1 {
				kp.Tables[j].Optimize = true
			}
		}
	}

	return nil
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

	// All tables in the same keyspace share the same replicas and ranges
	Tables []tablePlan
	// Idx of currently repaired table
	Idx       int
	AllTables bool

	Replicas []scyllaclient.ReplicaTokenRanges
	// Maps token range to replica set (by index) that owns it.
	// Contains all token ranges as entries.
	TokenRepIdx map[scyllaclient.TokenRange]int
}

func (kp keyspacePlan) IsReplicaMarked(repIdx, tabIdx int) bool {
	return len(kp.Replicas[repIdx].Ranges) == kp.Tables[tabIdx].MarkedInReplica[repIdx]
}

func (kp keyspacePlan) IsTableRepaired(tabIdx int) bool {
	return len(kp.TokenRepIdx) == kp.Tables[tabIdx].Done
}

// GetRangesToRepair returns at most cnt ranges of table owned by replica set.
func (kp keyspacePlan) GetRangesToRepair(repIdx, tabIdx, cnt int) []scyllaclient.TokenRange {
	rep := kp.Replicas[repIdx]
	tp := kp.Tables[tabIdx]

	// Return all ranges for optimized or deleted table
	if tp.Optimize || tp.Deleted {
		cnt = len(rep.Ranges)
	}

	var out []scyllaclient.TokenRange
	for _, r := range rep.Ranges {
		if tp.MarkRange(repIdx, r) {
			out = append(out, r)
			if len(out) >= cnt {
				break
			}
		}
	}

	return out
}

// tablePlan describes repair schedule and state for table.
type tablePlan struct {
	Table string
	Size  int64
	// Deleted tables are still being sent to workers,
	// so that their progress can still be updated in a fake way,
	// as their jobs are not actually sent to Scylla.
	Deleted bool
	// Optimized tables (small and fully replicated)
	// have all ranges for replica set repaired in a single job.
	Optimize bool
	// Marks scheduled ranges.
	MarkedRanges map[scyllaclient.TokenRange]struct{}
	// Marks amount of scheduled ranges in replica set (by index).
	MarkedInReplica []int
	// Amount of scheduled and finished ranges.
	Done int
}

// MarkRange sets range as done for replica.
func (tp tablePlan) MarkRange(repIdx int, r scyllaclient.TokenRange) bool {
	if _, ok := tp.MarkedRanges[r]; !ok {
		tp.MarkedRanges[r] = struct{}{}
		tp.MarkedInReplica[repIdx]++
		return true
	}
	return false
}

// filteredHosts returns hosts passing '--dc' and '--ignore-down-hosts' criteria.
func filteredHosts(ctx context.Context, target Target, client *scyllaclient.Client) (*strset.Set, error) {
	status, err := client.Status(ctx)
	if err != nil {
		return nil, errors.Wrap(err, "get status")
	}

	ignoredHosts := strset.New(target.IgnoreHosts...)
	dcs := strset.New(target.DC...)
	filtered := strset.New()

	for _, node := range status {
		if !ignoredHosts.Has(node.Addr) && dcs.Has(node.Datacenter) {
			filtered.Add(node.Addr)
		}
	}

	return filtered, nil
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
