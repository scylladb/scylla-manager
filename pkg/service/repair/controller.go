// Copyright (C) 2017 ScyllaDB

package repair

import (
	"math"
	"strconv"

	"github.com/scylladb/go-set/strset"
)

// controller informs generator if repair can be executed on a given set of
// hosts and if so how manny ranges can be repaired.
type controller interface {
	TryBlock(hosts []string) (bool, allowance)
	Unblock(a allowance)
	Busy() bool
	MaxWorkerCount() int
}

// defaultController ensures that there is at most one repair running at any node.
// If parallel is set it caps the number of replica sets that can be
// repaired at the same time.
// If intensity is set it changes the number of ranges in allowance returned
// when blocking replicas.
type defaultController struct {
	intensity *intensityHandler
	limits    hostRangesLimit

	busy *strset.Set
	jobs int
}

var _ controller = &defaultController{}

func newDefaultController(ih *intensityHandler, hl hostRangesLimit) *defaultController {
	if ih.MaxParallel() == 0 {
		panic("No available workers")
	}

	return &defaultController{
		intensity: ih,
		limits:    hl,
		busy:      strset.New(),
	}
}

func (c *defaultController) TryBlock(hosts []string) (bool, allowance) {
	if !c.shouldBlock(hosts) {
		return false, nilAllowance
	}

	a := c.allowance(hosts)
	c.block(hosts)
	return true, a
}

func (c *defaultController) shouldBlock(hosts []string) bool {
	// ALLOW if nothing is running
	if c.busy.IsEmpty() {
		return true
	}

	// DENY if there is too much parallel jobs running
	if c.jobs >= c.parallelLimit() {
		return false
	}

	// DENY if any host is already being repaired
	return !c.busy.HasAny(hosts...)
}

func (c *defaultController) parallelLimit() int {
	p := c.intensity.Parallel()

	if p == defaultParallel || p > c.intensity.MaxParallel() {
		return c.intensity.MaxParallel()
	}

	return p
}

func (c *defaultController) block(hosts []string) {
	c.busy.Add(hosts...)
	c.jobs++
}

func (c *defaultController) allowance(hosts []string) allowance {
	i := c.intensity.Intensity()

	a := allowance{
		Replicas: hosts,
		Ranges:   math.MaxInt32,
	}
	if i < 1 {
		a.ShardsPercent = i
	}

	for _, h := range hosts {
		var v int
		switch {
		case i == maxIntensity:
			v = c.limits[h].Max
		case i < 1:
			v = c.limits[h].Default
		default:
			v = int(i) * c.limits[h].Default
		}
		if v < a.Ranges {
			a.Ranges = v
		}
	}

	return a
}

func (c *defaultController) Unblock(a allowance) {
	c.busy.Remove(a.Replicas...)
	c.jobs--
}

func (c *defaultController) Busy() bool {
	return c.jobs > 0
}

func (c *defaultController) MaxWorkerCount() int {
	return c.intensity.MaxParallel()
}

// rowLevelRepairController is a specialised controller for row-level repair.
// While defaultController sets token ranges equal to the number of shards times
// intensity in one allowance the rowLevelRepairController splits that to the
// number of shards into chunks that can be executed independently.
// Each node can handle at most one repair task per shard.
// The number of ranges returned in allocation typically equals the intensity.
// If parallel is set it caps the number of distinct replica sets that can be
// repaired at the same time.
type rowLevelRepairController struct {
	intensity      *intensityHandler
	limits         hostRangesLimit
	maxWorkerCount int

	allJobs      int
	jobs         map[string]int
	busyRanges   map[string]int
	busyReplicas map[uint64]int
}

var _ controller = &rowLevelRepairController{}

func newRowLevelRepairController(ih *intensityHandler, hl hostRangesLimit, nodes, minRf int) *rowLevelRepairController {
	if ih.MaxParallel() == 0 {
		panic("No available workers")
	}
	if hl.MaxShards() == 0 {
		panic("No available ranges")
	}

	return &rowLevelRepairController{
		intensity:      ih,
		limits:         hl,
		maxWorkerCount: hl.MaxShards() * nodes / minRf,
		jobs:           make(map[string]int),
		busyRanges:     make(map[string]int),
		busyReplicas:   make(map[uint64]int),
	}
}

func (c *rowLevelRepairController) TryBlock(hosts []string) (bool, allowance) {
	i := c.intensity.Intensity()
	if !c.shouldBlock(hosts, i) {
		return false, nilAllowance
	}

	a := c.allowance(hosts, i)
	c.block(hosts, a.Ranges)
	return true, a
}

func (c *rowLevelRepairController) shouldBlock(hosts []string, intensity float64) bool {
	// ALLOW if nothing is running
	if len(c.busyReplicas) == 0 {
		return true
	}

	// DENY if any host has max nr of jobs already running
	for _, h := range hosts {
		if c.jobs[h] >= c.limits[h].Default {
			return false
		}
	}

	// DENY if any host already runs at the full intensity.
	// This may happen if we decrease intensity, for a while we need to reduce
	// number of jobs to reduce number of busy ranges.
	ranges := c.rangesForIntensity(hosts, intensity)
	for _, h := range hosts {
		var (
			r  = c.busyRanges[h] + ranges
			ok bool
		)
		switch {
		case intensity == maxIntensity:
			ok = r <= c.limits[h].Max
		case intensity <= 1:
			ok = r <= c.limits[h].Default
		default:
			ok = r <= int(intensity)*c.limits[h].Default && r <= c.limits[h].Max
		}
		if !ok {
			return false
		}
	}

	// DENY if there is too much parallel replicas being repaired
	if parallel := c.intensity.Parallel(); parallel != defaultParallel {
		hash := replicaHash(hosts)
		if _, ok := c.busyReplicas[hash]; !ok {
			l := len(c.busyReplicas) + 1
			if l > parallel || l > c.intensity.MaxParallel() {
				return false
			}
		}
	}

	// DENY if all workers are busy
	if c.allJobs == c.maxWorkerCount {
		return false
	}

	return true
}

func (c *rowLevelRepairController) block(hosts []string, ranges int) {
	c.allJobs++
	for _, h := range hosts {
		c.jobs[h]++
		c.busyRanges[h] += ranges
	}
	c.busyReplicas[replicaHash(hosts)]++
}

func (c *rowLevelRepairController) allowance(hosts []string, intensity float64) allowance {
	a := allowance{
		Replicas: hosts,
		Ranges:   c.rangesForIntensity(hosts, intensity),
	}
	if intensity < 1 {
		a.ShardsPercent = intensity
	}
	return a
}

func (c *rowLevelRepairController) rangesForIntensity(hosts []string, intensity float64) (ranges int) {
	switch {
	case intensity == maxIntensity:
		ranges = math.MaxInt32
		for _, h := range hosts {
			if v := c.limits[h].Max / c.limits[h].Default; v < ranges {
				ranges = v
			}
		}
		if ranges == 0 {
			ranges = 1
		}
	case intensity < 1:
		// If intensity < 1 return all token ranges (nr. of shards) in a single
		// call. This is to avoid multiple workers repairing the same host in
		// parallel. Single worker will offload work from shards using
		// the legacy repair logic i.e. repair shard by shard.
		ranges = math.MaxInt32
		for _, h := range hosts {
			if v := c.limits[h].Default; v < ranges {
				ranges = v
			}
		}
	default:
		ranges = int(intensity)
	}
	return
}

func (c *rowLevelRepairController) Unblock(a allowance) {
	c.allJobs--
	for _, h := range a.Replicas {
		c.jobs[h]--
		c.busyRanges[h] -= a.Ranges
	}

	hash := replicaHash(a.Replicas)
	switch c.busyReplicas[hash] {
	case 0:
		panic("Missing entry for hash" + strconv.FormatUint(hash, 10))
	case 1:
		delete(c.busyReplicas, hash)
	default:
		c.busyReplicas[hash]--
	}
}

func (c *rowLevelRepairController) Busy() bool {
	return len(c.busyReplicas) > 0
}

func (c *rowLevelRepairController) MaxWorkerCount() int {
	return c.maxWorkerCount
}
