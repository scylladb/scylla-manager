// Copyright (C) 2017 ScyllaDB

package repair

// controller informs generator about the amount of ranges that can be repaired
// on a given replica set. Returns 0 ranges when repair shouldn't be scheduled.
type controller interface {
	Block(replicaSet []string, todoRangesCnt int)
	TryBlock(replicaSet []string, todoRangesCnt int) (rangesCnt int, ok bool)
	Unblock(replicaSet []string, ranges int)
	Busy() bool
}

type intensityChecker interface {
	Intensity() Intensity
	Parallel() int
	MaxParallel() int
	ReplicaSetMinRepairRangesInParallel(replicaSet []string) int
	MaxJobsPerHost() int
}

// rowLevelRepairController is a specialised controller for row-level repair.
// It allows for at most '--parallel' repair jobs running in the cluster and
// at most '--max-jobs-per-host' running on every node at any time.
// It also ensures that at most '--intensity' ranges are repaired on every node at any time.
type rowLevelRepairController struct {
	intensity intensityChecker

	jobsCnt    int            // Total amount of repair jobs in the cluster
	nodeJobs   map[string]int // Amount of repair jobs currently executed on a given node
	nodeRanges map[string]int // Amount of token ranges currently repaired on a given node
}

var _ controller = &rowLevelRepairController{}

func newRowLevelRepairController(i intensityChecker) *rowLevelRepairController {
	return &rowLevelRepairController{
		intensity:  i,
		nodeJobs:   make(map[string]int),
		nodeRanges: make(map[string]int),
	}
}

func (c *rowLevelRepairController) TryBlock(replicaSet []string, todoRangesCnt int) (int, bool) {
	rangesCnt, ok := c.shouldBlock(replicaSet, todoRangesCnt)
	if !ok {
		return 0, false
	}
	c.Block(replicaSet, rangesCnt)
	return rangesCnt, true
}

func (c *rowLevelRepairController) shouldBlock(replicaSet []string, todoRangesCnt int) (int, bool) {
	// DENY if any node is already participating in --max-jobs-per-host jobs
	for _, r := range replicaSet {
		if c.nodeJobs[r] >= c.intensity.MaxJobsPerHost() {
			return 0, false
		}
	}

	// DENY if there are already '--parallel' repair jobs running
	parallel := c.intensity.Parallel()
	if parallel != defaultParallel && c.jobsCnt >= parallel {
		return 0, false
	}

	intensity := int(c.intensity.Intensity())
	if intensity == int(maxIntensity) {
		intensity = c.intensity.ReplicaSetMinRepairRangesInParallel(replicaSet)
	}

	rangesCnt := todoRangesCnt
	for _, r := range replicaSet {
		rangesCnt = min(intensity-c.nodeRanges[r], rangesCnt)
	}
	if rangesCnt <= 0 {
		return 0, false
	}

	return rangesCnt, true
}

func (c *rowLevelRepairController) Block(replicaSet []string, rangesCnt int) {
	c.jobsCnt++
	for _, r := range replicaSet {
		c.nodeJobs[r]++
		c.nodeRanges[r] += rangesCnt
	}
}

func (c *rowLevelRepairController) Unblock(replicaSet []string, rangesCnt int) {
	c.jobsCnt--
	for _, r := range replicaSet {
		c.nodeJobs[r]--
		c.nodeRanges[r] -= rangesCnt
	}
}

func (c *rowLevelRepairController) Busy() bool {
	return c.jobsCnt > 0
}
