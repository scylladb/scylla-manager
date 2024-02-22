// Copyright (C) 2017 ScyllaDB

package repair

import (
	"context"
	"math"
	"sort"
	"sync/atomic"

	"github.com/pkg/errors"
	"github.com/scylladb/go-log"
	"github.com/scylladb/scylla-manager/v3/pkg/dht"
	"github.com/scylladb/scylla-manager/v3/pkg/scyllaclient"
	"github.com/scylladb/scylla-manager/v3/pkg/util/slice"
)

// masterSelector describes each host priority for being repair master.
// Repair master is first chosen by smallest shard count,
// then by smallest dc RTT from SM.
type masterSelector map[string]int

func newMasterSelector(shards map[string]uint, hostDC map[string]string, closestDC []string) masterSelector {
	hosts := make([]string, 0, len(shards))
	for h := range shards {
		hosts = append(hosts, h)
	}

	sort.Slice(hosts, func(i, j int) bool {
		if shards[hosts[i]] != shards[hosts[j]] {
			return shards[hosts[i]] < shards[hosts[j]]
		}
		return slice.Index(closestDC, hostDC[hosts[i]]) < slice.Index(closestDC, hostDC[hosts[j]])
	})

	ms := make(masterSelector)
	for i, h := range hosts {
		ms[h] = i
	}
	return ms
}

// Select returns repair master from replica set.
func (ms masterSelector) Select(replicas []string) string {
	var master string
	p := math.MaxInt64
	for _, r := range replicas {
		if ms[r] < p {
			p = ms[r]
			master = r
		}
	}
	return master
}

type submitter[T, R any] interface {
	Submit(task T)
	Results() chan R
	Close()
}

type job struct {
	keyspace   string
	table      string
	master     string
	replicaSet []string
	ranges     []scyllaclient.TokenRange
	// jobs of optimized tables should merge all token ranges into one.
	optimize bool
	// jobs of deleted tables are sent only for progress updates.
	deleted bool
}

// tryOptimizeRanges returns either predefined ranges
// or one full token range for small fully replicated tables.
func (j job) tryOptimizeRanges() []scyllaclient.TokenRange {
	if j.optimize {
		return []scyllaclient.TokenRange{
			{
				StartToken: dht.Murmur3MinToken,
				EndToken:   dht.Murmur3MaxToken,
			},
		}
	}
	return j.ranges
}

type jobResult struct {
	job
	err error
}

func (r jobResult) Success() bool {
	// jobs of deleted tables are considered to by successful
	return r.err == nil || errors.Is(r.err, errTableDeleted)
}

// Tools shared between generator and tableGenerator.
type generatorTools struct {
	ctl       controller
	ms        masterSelector
	submitter submitter[job, jobResult]
	failFast  bool
	stop      *atomic.Bool
	logger    log.Logger
}

func (gt generatorTools) stopGenerating() {
	gt.stop.Store(true)
}

// generator is responsible for creating and orchestrating tableGenerators.
type generator struct {
	generatorTools

	target Target
	plan   *plan
	pm     ProgressManager
	client *scyllaclient.Client
}

func (g *generator) shouldGenerate() bool {
	return !g.stop.Load()
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
			ctl:       newRowLevelRepairController(i),
			ms:        newMasterSelector(shards, status.HostDC(), closestDC),
			submitter: s,
			failFast:  target.FailFast,
			stop:      &atomic.Bool{},
			logger:    logger,
		},
		target: target,
		plan:   plan,
		pm:     pm,
		client: client,
	}, nil
}

func (g *generator) Run(ctx context.Context) error {
	g.logger.Info(ctx, "Start generator")
	status, err := g.client.Status(ctx)
	if err != nil {
		return errors.Wrap(err, "get status")
	}
	var genErr error

	for _, ksp := range g.plan.Keyspaces {
		if !g.shouldGenerate() {
			break
		}

		ring, err := g.client.DescribeVnodeRing(ctx, ksp.Keyspace)
		if err != nil {
			return errors.Wrap(err, "describe ring")
		}
		// Make sure that replica sets are filtered as we might not include
		// all hosts in size/ranges/intensity calculations.
		_ = filteredRing(g.target, status, ring)

		for _, tp := range ksp.Tables {
			if !g.shouldGenerate() {
				break
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
	done := g.pm.GetDoneRanges(keyspace, tp.Table)
	for _, r := range done {
		delete(todoRanges, r)
	}

	tg := &tableGenerator{
		generatorTools: g.generatorTools,
		Keyspace:       keyspace,
		Table:          tp.Table,
		Ring:           ring,
		TodoRanges:     todoRanges,
		DoneReplicas:   make(map[uint64]struct{}),
		Optimize:       tp.Optimize,
	}
	tg.logger = tg.logger.Named(keyspace + "." + tp.Table)
	return tg
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
	Optimize     bool
	Deleted      bool
	Err          error
}

func (tg *tableGenerator) Run(ctx context.Context) error {
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

func (tg *tableGenerator) processResult(ctx context.Context, jr jobResult) {
	// Don't record context errors
	if errors.Is(jr.err, context.Canceled) {
		return
	}

	if jr.err != nil && errors.Is(jr.err, errTableDeleted) {
		tg.logger.Info(ctx, "Detected table deletion", "keyspace", jr.keyspace, "table", jr.table)
		tg.Deleted = true
	}

	if !jr.Success() {
		tg.logger.Error(ctx, "Repair failed", "error", jr.err)
		// All errors are logged, so in order to reduce clutter,
		// return only the first one.
		if tg.Err == nil {
			tg.Err = jr.err
		}
		if tg.failFast {
			tg.stopGenerating()
		}
	}
	tg.ctl.Unblock(jr.replicaSet)
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
		repHash := scyllaclient.ReplicaHash(rt.ReplicaSet)
		if _, ok := tg.DoneReplicas[repHash]; ok {
			continue
		}

		if cnt := tg.ctl.TryBlock(rt.ReplicaSet); cnt > 0 {
			ranges := tg.getRangesToRepair(rt.Ranges, cnt)
			if len(ranges) == 0 {
				tg.DoneReplicas[repHash] = struct{}{}
				tg.ctl.Unblock(rt.ReplicaSet)
				continue
			}

			return job{
				keyspace:   tg.Keyspace,
				table:      tg.Table,
				master:     tg.ms.Select(rt.ReplicaSet),
				replicaSet: rt.ReplicaSet,
				ranges:     ranges,
				optimize:   tg.Optimize,
				deleted:    tg.Deleted,
			}, true
		}
	}

	return job{}, false
}

func (tg *tableGenerator) getRangesToRepair(allRanges []scyllaclient.TokenRange, cnt Intensity) []scyllaclient.TokenRange {
	if tg.Optimize || tg.Deleted {
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

func (tg *tableGenerator) shouldGenerate() bool {
	return !tg.stop.Load() && len(tg.TodoRanges) > 0
}

func (tg *tableGenerator) shouldExit() bool {
	return !tg.ctl.Busy() && !tg.shouldGenerate()
}
