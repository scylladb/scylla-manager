// Copyright (C) 2017 ScyllaDB

package repair

// controller informs generator about the amount of ranges that can be repaired
// on a given replica set. Returns 0 ranges when repair shouldn't be scheduled.
type controller interface {
	TryBlock(replicaSet []string) (ranges Intensity)
	Unblock(replicaSet []string)
	Busy() bool
}

type intensityChecker interface {
	Intensity() Intensity
	Parallel() int
	MaxParallel() int
	ReplicaSetMaxIntensity(replicaSet []string) Intensity
}

// rowLevelRepairController is a specialised controller for row-level repair.
// It allows for at most '--parallel' repair jobs running in the cluster and
// at most one job running on every node at any time.
// It always returns either 0 or '--intensity' ranges.
type rowLevelRepairController struct {
	intensity intensityChecker

	jobsCnt  int            // Total amount of repair jobs in the cluster
	nodeJobs map[string]int // Amount of repair jobs on a given node
}

var _ controller = &rowLevelRepairController{}

func newRowLevelRepairController(i intensityChecker) *rowLevelRepairController {
	return &rowLevelRepairController{
		intensity: i,
		nodeJobs:  make(map[string]int),
	}
}

func (c *rowLevelRepairController) TryBlock(replicaSet []string) Intensity {
	if !c.shouldBlock(replicaSet) {
		return 0
	}
	c.block(replicaSet)

	i := c.intensity.Intensity()
	if maxI := c.intensity.ReplicaSetMaxIntensity(replicaSet); i == maxIntensity || maxI < i {
		i = maxI
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

func (c *rowLevelRepairController) block(replicaSet []string) {
	c.jobsCnt++
	for _, r := range replicaSet {
		c.nodeJobs[r]++
	}
}

func (c *rowLevelRepairController) Unblock(replicaSet []string) {
	c.jobsCnt--
	for _, r := range replicaSet {
		c.nodeJobs[r]--
	}
}

func (c *rowLevelRepairController) Busy() bool {
	return c.jobsCnt > 0
}
