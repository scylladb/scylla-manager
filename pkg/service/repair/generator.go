// Copyright (C) 2017 ScyllaDB

package repair

import (
	"context"
	stdErrors "errors"
	"sync/atomic"

	"github.com/pkg/errors"
	"github.com/scylladb/go-log"
	"github.com/scylladb/scylla-manager/v3/pkg/scyllaclient"
)

// generator is responsible for creating and orchestrating tableGenerators.
type generator struct {
	generatorTools

	target Target
	plan   *plan
	pm     ProgressManager
	client *scyllaclient.Client
}

// tableGenerator is responsible for generating and orchestrating
// repair jobs of given table.
type tableGenerator struct {
	generatorTools

	Keyspace     string
	Table        string
	Ring         scyllaclient.Ring
	TodoRanges   map[scyllaclient.TokenRange]struct{}
	DoneReplicas map[uint64]struct{}
	JobType      jobType
	Err          error
}

// Tools shared between generator and tableGenerator.
type generatorTools struct {
	target        Target
	ctl           controller
	ms            masterSelector
	submitter     submitter[job, jobResult]
	ringDescriber scyllaclient.RingDescriber
	stop          *atomic.Bool
	logger        log.Logger
}

type submitter[T, R any] interface {
	Submit(task T)
	Results() chan R
	Close()
}

// jobType describes how worker should handle given repair job.
type jobType int

const (
	normalJobType jobType = iota
	skipJobType
	mergeRangesJobType
)

type job struct {
	keyspace   string
	table      string
	master     string
	replicaSet []string
	ranges     []scyllaclient.TokenRange
	jobType    jobType
}

type jobResult struct {
	job
	err error
}

func newGenerator(ctx context.Context, target Target, client *scyllaclient.Client, i intensityChecker,
	s submitter[job, jobResult], plan *plan, pm ProgressManager, logger log.Logger,
) (*generator, error) {
	status, err := client.Status(ctx)
	if err != nil {
		return nil, errors.Wrap(err, "get status")
	}
	closestDC, err := client.ClosestDC(ctx, status.DatacenterMap(target.DC))
	if err != nil {
		return nil, errors.Wrap(err, "calculate closest dc")
	}

	shards, err := client.HostsShardCount(ctx, plan.Hosts)
	if err != nil {
		return nil, err
	}

	return &generator{
		generatorTools: generatorTools{
			target:        target,
			ctl:           newRowLevelRepairController(i),
			ms:            newMasterSelector(shards, status.HostDC(), closestDC),
			submitter:     s,
			ringDescriber: scyllaclient.NewRingDescriber(ctx, client),
			stop:          &atomic.Bool{},
			logger:        logger,
		},
		target: target,
		plan:   plan,
		pm:     pm,
		client: client,
	}, nil
}

func (g *generator) Run(ctx context.Context) (err error) {
	g.logger.Info(ctx, "Start generator")
	var genErr error

	defer func() {
		// Always leave tablet migration enabled after repair
		err = stdErrors.Join(err, g.ringDescriber.ControlTabletLoadBalancing(context.Background(), true))
	}()

	for _, ksp := range g.plan.Keyspaces {
		// Disable tablet migration when repairing tablet table.
		// Without that it could be possible that some tablet "escapes" being
		// a repaired by migrating from not yet repaired token range to already repaired one.
		if err := g.ringDescriber.ControlTabletLoadBalancing(ctx, g.ringDescriber.IsTabletKeyspace(ksp.Keyspace)); err != nil {
			return errors.Wrapf(err, "control tablet load balancing")
		}

		for _, tp := range ksp.Tables {
			if !g.shouldGenerate() {
				break
			}

			ring, err := g.ringDescriber.DescribeRing(ctx, ksp.Keyspace, tp.Table)
			if err != nil {
				return errors.Wrap(err, "describe ring")
			}

			tg := g.newTableGenerator(ksp.Keyspace, tp, ring)
			// All errors are logged, so in order to reduce clutter,
			// return only the first one.
			if err := tg.Run(ctx); err != nil && genErr == nil {
				genErr = err
			}
		}
	}

	g.logger.Info(ctx, "Close generator")
	g.submitter.Close() // Free workers waiting on next
	return errors.Wrap(genErr, "see more errors in logs")
}

func (g *generator) newTableGenerator(keyspace string, tp tablePlan, ring scyllaclient.Ring) *tableGenerator {
	todoRanges := make(map[scyllaclient.TokenRange]struct{})
	for _, rt := range ring.ReplicaTokens {
		for _, r := range rt.Ranges {
			todoRanges[r] = struct{}{}
		}
	}

	done, allRangesCnt := g.pm.GetCompletedRanges(keyspace, tp.Table)
	// Always repair unfinished tablet table from scratch as
	// tablet load balancing is enabled when repair is interrupted.
	if !g.ringDescriber.IsTabletKeyspace(keyspace) || len(done) == allRangesCnt {
		for _, r := range done {
			delete(todoRanges, r)
		}
	}

	var jt jobType
	switch {
	case len(ring.ReplicaTokens) == 1 && tp.Small:
		jt = mergeRangesJobType
	default:
		jt = normalJobType
	}

	tg := &tableGenerator{
		generatorTools: g.generatorTools,
		Keyspace:       keyspace,
		Table:          tp.Table,
		Ring:           ring,
		TodoRanges:     todoRanges,
		DoneReplicas:   make(map[uint64]struct{}),
		JobType:        jt,
	}
	tg.logger = tg.logger.Named(keyspace + "." + tp.Table)
	return tg
}

func (g *generator) shouldGenerate() bool {
	return !g.stop.Load()
}

func (tg *tableGenerator) Run(ctx context.Context) error {
	if len(tg.TodoRanges) == 0 {
		tg.logger.Info(ctx, "All ranges are already repaired")
		return nil
	}

	tg.logger.Info(ctx, "Start table generator")
	tg.generateJobs()
	for !tg.shouldExit() {
		if ctx.Err() != nil {
			break
		}
		select {
		case <-ctx.Done():
		case r := <-tg.submitter.Results():
			tg.processResult(ctx, r)
			tg.generateJobs()
		}
	}
	tg.logger.Info(ctx, "Close table generator")
	return tg.Err
}

func (tg *tableGenerator) generateJobs() {
	for {
		j, ok := tg.newJob()
		if !ok {
			return
		}
		tg.submitter.Submit(j)
	}
}

// newJob tries to return job passing controller restrictions.
func (tg *tableGenerator) newJob() (job, bool) {
	if !tg.shouldGenerate() {
		return job{}, false
	}

	for _, rt := range tg.Ring.ReplicaTokens {
		// Calculate replica hash on not filtered replica set
		// because different replica sets might be the same after filtering.
		repHash := scyllaclient.ReplicaHash(rt.ReplicaSet)
		if _, ok := tg.DoneReplicas[repHash]; ok {
			continue
		}

		filtered := filterReplicaSet(rt.ReplicaSet, tg.Ring.HostDC, tg.target)
		if len(filtered) == 0 {
			tg.DoneReplicas[repHash] = struct{}{}
			for _, r := range rt.Ranges {
				delete(tg.TodoRanges, r)
			}
			continue
		}

		if cnt := tg.ctl.TryBlock(filtered); cnt > 0 {
			ranges := tg.getRangesToRepair(rt.Ranges, cnt)
			if len(ranges) == 0 {
				tg.DoneReplicas[repHash] = struct{}{}
				tg.ctl.Unblock(filtered)
				continue
			}

			return job{
				keyspace:   tg.Keyspace,
				table:      tg.Table,
				master:     tg.ms.Select(filtered),
				replicaSet: filtered,
				ranges:     ranges,
				jobType:    tg.JobType,
			}, true
		}
	}

	return job{}, false
}

func (tg *tableGenerator) getRangesToRepair(allRanges []scyllaclient.TokenRange, cnt Intensity) []scyllaclient.TokenRange {
	if tg.JobType != normalJobType {
		cnt = NewIntensity(len(allRanges))
	}

	var ranges []scyllaclient.TokenRange
	for _, r := range allRanges {
		if _, ok := tg.TodoRanges[r]; !ok {
			continue
		}
		delete(tg.TodoRanges, r)
		ranges = append(ranges, r)
		if NewIntensity(len(ranges)) >= cnt {
			break
		}
	}

	return ranges
}

func (tg *tableGenerator) processResult(ctx context.Context, jr jobResult) {
	// Don't record context errors
	if errors.Is(jr.err, context.Canceled) {
		return
	}

	if jr.err != nil && errors.Is(jr.err, errTableDeleted) {
		tg.logger.Info(ctx, "Detected table deletion", "keyspace", jr.keyspace, "table", jr.table)
		// Remaining jobs from deleted table are skipped
		// (and sent only for recording progress).
		tg.JobType = skipJobType
	}

	if !jr.Success() {
		tg.logger.Error(ctx, "Repair failed", "error", jr.err)
		// All errors are logged, so in order to reduce clutter,
		// return only the first one.
		if tg.Err == nil {
			tg.Err = jr.err
		}
		if tg.target.FailFast {
			tg.stopGenerating()
		}
	}
	tg.ctl.Unblock(jr.replicaSet)
}

func (gt generatorTools) stopGenerating() {
	gt.stop.Store(true)
}

func (tg *tableGenerator) shouldGenerate() bool {
	return !tg.stop.Load() && len(tg.TodoRanges) > 0
}

func (tg *tableGenerator) shouldExit() bool {
	return !tg.ctl.Busy() && !tg.shouldGenerate()
}

func (r jobResult) Success() bool {
	// jobs of deleted tables are considered to by successful
	return r.err == nil || errors.Is(r.err, errTableDeleted)
}
