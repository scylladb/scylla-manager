// Copyright (C) 2017 ScyllaDB

package repair

import "net/netip"

// controller keeps the state of repairs running in the cluster
// and informs generator about allowed repair intensity on a given replica set.
type controller interface {
	// TryBlock returns if it's allowed to schedule a repair job on given replica set.
	// The second returned value is the allowed intensity of such job.
	TryBlock(replicaSet []netip.Addr) (ok bool, intensity int)
	// Unblock informs controller that a repair job running on replica set has finished.
	// This makes it possible to call TryBlock on nodes from replica set.
	Unblock(replicaSet []netip.Addr)
	// Busy checks if there are any running repair jobs that controller is aware of.
	Busy() bool
}

type intensityChecker interface {
	Intensity() Intensity
	Parallel() int
	MaxParallel() int
	ReplicaSetMaxIntensity(replicaSet []netip.Addr) Intensity
}

// rowLevelRepairController is a specialised controller for row-level repair.
// It allows for at most '--parallel' repair jobs running in the cluster and
// at most one job running on every node at any time.
// It always returns either 0 or '--intensity' ranges.
type rowLevelRepairController struct {
	intensity intensityChecker

	jobsCnt  int                // Total amount of repair jobs in the cluster
	nodeJobs map[netip.Addr]int // Amount of repair jobs on a given node
}

var _ controller = &rowLevelRepairController{}

func newRowLevelRepairController(i intensityChecker) *rowLevelRepairController {
	return &rowLevelRepairController{
		intensity: i,
		nodeJobs:  make(map[netip.Addr]int),
	}
}

func (c *rowLevelRepairController) TryBlock(replicaSet []netip.Addr) (ok bool, intensity int) {
	if !c.shouldBlock(replicaSet) {
		return false, 0
	}
	c.block(replicaSet)

	i := c.intensity.Intensity()
	if maxI := c.intensity.ReplicaSetMaxIntensity(replicaSet); i == maxIntensity || maxI < i {
		i = maxI
	}
	return true, int(i)
}

func (c *rowLevelRepairController) shouldBlock(replicaSet []netip.Addr) bool {
	// DENY if any node is already participating in repair job
	for _, r := range replicaSet {
		if c.nodeJobs[r] > 0 {
			return false
		}
	}

	// DENY if there are already '--parallel' repair jobs running
	parallel := c.intensity.Parallel()
	if parallel != defaultParallel && c.jobsCnt >= parallel {
		return false
	}
	// DENY if it's trying to exceed maxParallel
	if parallel == defaultParallel && c.jobsCnt >= c.intensity.MaxParallel() {
		return false
	}

	return true
}

func (c *rowLevelRepairController) block(replicaSet []netip.Addr) {
	c.jobsCnt++
	for _, r := range replicaSet {
		c.nodeJobs[r]++
	}
}

func (c *rowLevelRepairController) Unblock(replicaSet []netip.Addr) {
	c.jobsCnt--
	for _, r := range replicaSet {
		c.nodeJobs[r]--
	}
}

func (c *rowLevelRepairController) Busy() bool {
	return c.jobsCnt > 0
}
