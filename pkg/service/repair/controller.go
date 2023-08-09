// Copyright (C) 2017 ScyllaDB

package repair

import (
	"math"
)

// controller informs generator if repair can be executed on a given set of
// hosts and if so how manny ranges can be repaired.
type controller interface {
	TryBlock(hosts []string) (bool, allowance)
	Unblock(a allowance)
	Busy() bool
	MaxWorkerCount() int
}

// rowLevelRepairController is a responsible for ensuring that just a single repair
// job is scheduled against single host. It guarantees as well that no more than X (--parallel flag)
// replicas are repaired in parallel.
// It's responsible for controlling amount of token ranges included into the job.
type rowLevelRepairController struct {
	intensity      *intensityHandler
	limits         hostRangesLimit
	maxWorkerCount int
	allJobs        int
	jobs           map[string]int
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
	}
}

func (c *rowLevelRepairController) TryBlock(hosts []string) (bool, allowance) {
	if !c.shouldBlock(hosts) {
		return false, nilAllowance
	}

	a := c.allowance(hosts, c.intensity.Intensity())
	c.block(hosts, a.Ranges)
	return true, a
}

func (c *rowLevelRepairController) shouldBlock(hosts []string) bool {
	// ALLOW if nothing is running
	if !c.Busy() {
		return true
	}

	// DENY if any host has a job already running
	for _, h := range hosts {
		if c.jobs[h] >= 1 {
			return false
		}
	}
	// DENY if there is too much parallel replicas being repaired
	parallelReplicas := c.intensity.Parallel()
	if parallelReplicas != defaultParallel && c.allJobs >= parallelReplicas {
		return false
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
	}
}

func (c *rowLevelRepairController) allowance(hosts []string, intensity int) allowance {
	a := allowance{
		Replicas: hosts,
		Ranges:   c.rangesForIntensity(hosts, intensity),
	}
	return a
}

func (c *rowLevelRepairController) rangesForIntensity(hosts []string, intensity int) (ranges int) {
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
	default:
		ranges = intensity
	}
	return
}

func (c *rowLevelRepairController) Unblock(a allowance) {
	c.allJobs--
	for _, h := range a.Replicas {
		c.jobs[h]--
	}
}

func (c *rowLevelRepairController) Busy() bool {
	return c.allJobs > 0
}

func (c *rowLevelRepairController) MaxWorkerCount() int {
	return c.maxWorkerCount
}
