// Copyright (C) 2017 ScyllaDB

package repair

import "math"

// controller informs generator about the amount of ranges that can be repaired
// on a given replica set. Returns 0 ranges when repair shouldn't be scheduled.
type controller interface {
	TryBlock(replicaSet []string) (ranges int)
	Unblock(replicaSet []string)
	MaxParallel() int
	Busy() bool
}

// rowLevelRepairController is a specialised controller for row-level repair.
// It allows for at most '--parallel' repair jobs running in the cluster and
// at most one job running on every node at any time.
// It always returns either 0 or '--intensity' ranges.
type rowLevelRepairController struct {
	intensity     *intensityHandler
	hostMaxRanges map[string]int // max_ranges_in_parallel limits for hosts
	maxParallel   int

	jobsCnt  int            // Total amount of repair jobs in the cluster
	nodeJobs map[string]int // Amount of repair jobs on a given node
}

var _ controller = &rowLevelRepairController{}

func newRowLevelRepairController(ih *intensityHandler, maxParallel int, hostMaxRanges map[string]int) *rowLevelRepairController {
	return &rowLevelRepairController{
		intensity:     ih,
		hostMaxRanges: hostMaxRanges,
		maxParallel:   maxParallel,
		nodeJobs:      make(map[string]int),
	}
}

func (c *rowLevelRepairController) TryBlock(replicaSet []string) int {
	if !c.shouldBlock(replicaSet) {
		return 0
	}
	c.block(replicaSet)

	i := c.intensity.Intensity()
	if i == maxIntensity {
		i = c.replicaMaxRanges(replicaSet)
	}
	return i
}

func (c *rowLevelRepairController) shouldBlock(replicaSet []string) bool {
	// DENY if any node is already participating in repair job
	for _, r := range replicaSet {
		if c.nodeJobs[r] > 0 {
			return false
		}
	}

	// DENY if there are already '--parallel' repair jobs running
	if parallel := c.intensity.Parallel(); parallel != defaultParallel {
		if c.jobsCnt >= parallel {
			return false
		}
	}

	return true
}

func (c *rowLevelRepairController) block(replicaSet []string) {
	c.jobsCnt++
	for _, r := range replicaSet {
		c.nodeJobs[r]++
	}
}

func (c *rowLevelRepairController) replicaMaxRanges(replicaSet []string) int {
	min := math.MaxInt
	for _, rep := range replicaSet {
		if ranges := c.hostMaxRanges[rep]; ranges < min {
			min = ranges
		}
	}
	return min
}

func (c *rowLevelRepairController) Unblock(replicaSet []string) {
	c.jobsCnt--
	for _, r := range replicaSet {
		c.nodeJobs[r]--
	}
}

func (c *rowLevelRepairController) MaxParallel() int {
	return c.maxParallel
}

func (c *rowLevelRepairController) Busy() bool {
	return c.jobsCnt > 0
}
