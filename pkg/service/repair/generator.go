// Copyright (C) 2017 ScyllaDB

package repair

import (
	"context"
	"math"
	"sort"
	"strings"
	"sync/atomic"

	"github.com/pkg/errors"
	"github.com/scylladb/go-log"
	"github.com/scylladb/go-set/strset"
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

type generator struct {
	plan   *plan
	ctl    controller
	ms     masterSelector
	client *scyllaclient.Client

	logger   log.Logger
	failFast bool

	// Jobs for workers.
	next chan job
	// Job results from workers.
	result chan jobResult
	// Determines if generator should keep on generating new jobs.
	stop atomic.Bool

	// Statistics for logging purposes.
	count       int
	success     int
	failed      int
	lastPercent int
}

func newGenerator(ctx context.Context, target Target,
	client *scyllaclient.Client, ih *intensityHandler, logger log.Logger,
) (*generator, error) {
	var ord, cnt int
	for _, kp := range target.plan.Keyspaces {
		for _, tp := range kp.Tables {
			cnt += len(kp.TokenRepIdx)
			ord++
			logger.Info(ctx, "Repair order",
				"order", ord,
				"keyspace", kp.Keyspace,
				"table", tp.Table,
				"size", tp.Size,
				"merge_ranges", tp.Optimize,
			)
		}
	}

	hosts := target.plan.Hosts()
	status, err := client.Status(ctx)
	if err != nil {
		return nil, errors.Wrap(err, "get status")
	}
	if down := strset.New(status.Down().Hosts()...); down.HasAny(hosts...) {
		return nil, errors.Errorf("ensure nodes are up, down nodes: %s", strings.Join(down.List(), ","))
	}
	closestDC, err := client.ClosestDC(ctx, status.DatacenterMap(target.DC))
	if err != nil {
		return nil, errors.Wrap(err, "calculate closest dc")
	}

	shards, err := client.HostsShardCount(ctx, hosts)
	if err != nil {
		return nil, err
	}

	return &generator{
		plan:        target.plan,
		ctl:         newRowLevelRepairController(ih),
		ms:          newMasterSelector(shards, status.HostDC(), closestDC),
		client:      client,
		next:        make(chan job, target.plan.MaxParallel),
		result:      make(chan jobResult, target.plan.MaxParallel),
		logger:      logger,
		failFast:    target.FailFast,
		count:       cnt,
		lastPercent: -1,
	}, nil
}

func (g *generator) Run(ctx context.Context) error {
	g.logger.Info(ctx, "Start generator")
	running := true

	g.generateJobs()
	if g.shouldExit() {
		running = false
	}

	for running {
		if ctx.Err() != nil {
			break
		}
		select {
		case <-ctx.Done():
			running = false
		case r := <-g.result:
			g.processResult(ctx, r)
			g.generateJobs()
			if g.shouldExit() {
				running = false
			}
		}
	}

	g.logger.Info(ctx, "Close generator")
	close(g.next) // Free workers waiting on next

	// Don't return ctx error as graceful ctx is handled from service level
	if g.failed > 0 {
		return errors.Errorf("repair %d token ranges out of %d", g.failed, g.count)
	}
	return nil
}

func (g *generator) processResult(ctx context.Context, r jobResult) {
	if r.err != nil && errors.Is(r.err, errTableDeleted) {
		g.logger.Info(ctx, "Detected table deletion", "keyspace", r.keyspace, "table", r.table)
		g.plan.MarkDeleted(r.keyspace, r.table)
	}

	g.plan.MarkDoneRanges(r.keyspace, r.table, len(r.ranges))
	if r.Success() {
		g.success += len(r.ranges)
	} else {
		g.logger.Error(ctx, "Repair failed", "error", r.err)
		g.failed += len(r.ranges)
		if g.failFast {
			g.stopGenerating()
		}
	}

	if percent := 100 * (g.success + g.failed) / g.count; percent > g.lastPercent {
		g.logger.Info(ctx, "Progress", "percent", percent, "count", g.count, "success", g.success, "failed", g.failed)
		g.lastPercent = percent
	}
	g.ctl.Unblock(r.replicaSet)
}

func (g *generator) generateJobs() {
	for {
		j, ok := g.newJob()
		if !ok {
			return
		}

		select {
		case g.next <- j:
		default:
			panic("next is full")
		}
	}
}

// newJob tries to return job passing controller restrictions.
func (g *generator) newJob() (job, bool) {
	if ok := g.plan.UpdateIdx(); !ok {
		g.stopGenerating()
	}
	if !g.shouldGenerate() {
		return job{}, false
	}

	ksIdx := g.plan.Idx
	kp := g.plan.Keyspaces[ksIdx]
	tabIdx := kp.Idx
	tp := kp.Tables[tabIdx]

	for repIdx, rep := range kp.Replicas {
		if kp.IsReplicaMarked(repIdx, tabIdx) {
			continue
		}

		if ranges := g.ctl.TryBlock(rep.ReplicaSet); ranges > 0 {
			return job{
				keyspace:   kp.Keyspace,
				table:      tp.Table,
				master:     g.ms.Select(rep.ReplicaSet),
				replicaSet: rep.ReplicaSet,
				ranges:     kp.GetRangesToRepair(repIdx, tabIdx, ranges),
				optimize:   tp.Optimize,
				deleted:    tp.Deleted,
			}, true
		}
	}

	return job{}, false
}

func (g *generator) stopGenerating() {
	g.stop.Store(true)
}

func (g *generator) shouldGenerate() bool {
	return !g.stop.Load()
}

func (g *generator) shouldExit() bool {
	return !g.ctl.Busy() && !g.shouldGenerate()
}
