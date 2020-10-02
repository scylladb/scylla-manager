// Copyright (C) 2017 ScyllaDB

package repair

import (
	"context"
	"math/rand"
	"strings"
	"time"

	"github.com/pkg/errors"
	"github.com/scylladb/go-log"
	"github.com/scylladb/go-set/strset"
)

// TODO add docs to types

type hostPriority map[string]int

func (hp hostPriority) PickHost(replicas []string) string {
	for p := 0; p < len(hp); p++ {
		for _, r := range replicas {
			if hp[r] == p {
				return r
			}
		}
	}
	return replicas[0]
}

type job struct {
	Host          string
	Ranges        []*tableTokenRange
	ShardsPercent float64
}

type jobResult struct {
	job
	Err error
}

type generator struct {
	gracefulStopTimeout time.Duration
	logger              log.Logger

	replicas         map[uint64][]string
	ranges           map[uint64][]*tableTokenRange
	hostCount        int
	hostPriority     hostPriority
	hostRangesLimits hostRangesLimit
	smallTables      *strset.Set
	deletedTables    *strset.Set
	progress         progressManager
	lastPercent      int

	intensityHandler *intensityHandler

	keys         []uint64
	pos          int
	busy         *strset.Set
	next         chan job
	nextClosed   bool
	result       chan jobResult
	failFast     bool
	inflightJobs int

	count   int
	success int
	failed  int
}

func newGenerator(gracefulStopTimeout time.Duration, manager progressManager, failFast bool, logger log.Logger) *generator {
	g := &generator{
		gracefulStopTimeout: gracefulStopTimeout,
		progress:            manager,
		failFast:            failFast,
		logger:              logger,

		replicas:      make(map[uint64][]string),
		ranges:        make(map[uint64][]*tableTokenRange),
		smallTables:   strset.New(),
		deletedTables: strset.New(),
		lastPercent:   -1,
	}

	return g
}

func (g *generator) Add(ranges []*tableTokenRange) {
	for _, ttr := range ranges {
		g.add(ttr)
	}
}

func (g *generator) add(ttr *tableTokenRange) {
	hash := ttr.ReplicaHash()
	g.replicas[hash] = ttr.Replicas
	g.ranges[hash] = append(g.ranges[hash], ttr)
}

func (g *generator) Hosts() *strset.Set {
	all := strset.New()
	for _, v := range g.replicas {
		all.Add(v...)
	}
	return all
}

func (g *generator) SetHostPriority(hp hostPriority) {
	g.hostPriority = hp
}

func (g *generator) SetHostRangeLimits(hl hostRangesLimit) {
	g.hostRangesLimits = hl
}

func (g *generator) Init(ctx context.Context, ih *intensityHandler) error {
	if ih.MaxParallel() == 0 {
		return errors.New("no available workers")
	}
	g.intensityHandler = ih
	if len(g.replicas) == 0 {
		return errors.New("no replicas to repair")
	}
	g.keys = make([]uint64, 0, len(g.replicas))
	for k := range g.replicas {
		g.keys = append(g.keys, k)
	}
	g.hostCount = g.Hosts().Size()
	g.pos = rand.Intn(len(g.keys))
	g.busy = strset.New()
	g.next = make(chan job, 2*ih.MaxParallel())
	g.result = make(chan jobResult)

	var trs []*tableTokenRange
	for _, ttrs := range g.ranges {
		trs = append(trs, ttrs...)
	}
	if err := g.progress.Init(ctx, trs); err != nil {
		return err
	}

	// Remove repaired ranges from the pool of available ones to avoid their
	// scheduling.
	for k := range g.ranges {
		ttrs := g.ranges[k][:0]
		for i := range g.ranges[k] {
			if g.progress.CheckRepaired(g.ranges[k][i]) {
				continue
			}
			ttrs = append(ttrs, g.ranges[k][i])
		}
		g.ranges[k] = ttrs
		g.count += len(ttrs)
	}

	return nil
}

func (g *generator) Next() <-chan job {
	return g.next
}

func (g *generator) Result() chan<- jobResult {
	return g.result
}

func (g *generator) Run(ctx context.Context) (err error) {
	g.logger.Info(ctx, "Start repair")

	g.fillNext(ctx)

	done := ctx.Done()
	stop := make(chan struct{})
loop:
	for {
		select {
		case <-stop:
			break loop
		case <-done:
			g.logger.Info(ctx, "Graceful repair shutdown", "timeout", g.gracefulStopTimeout)
			// Stop workers by closing next channel
			g.closeNext()

			done = nil
			time.AfterFunc(g.gracefulStopTimeout, func() {
				close(stop)
			})
			err = ctx.Err()
		case r := <-g.result:
			// TODO handling penalties
			g.processResult(ctx, r)
			g.fillNext(ctx)

			if done := g.busy.IsEmpty(); done {
				g.logger.Info(ctx, "Done repair")

				g.closeNext()
				break loop
			}
		}
	}

	if g.failed > 0 {
		return errors.Errorf("%d token ranges out of %d failed to repair", g.failed, g.count)
	}
	return err
}

func (g *generator) processResult(ctx context.Context, r jobResult) {
	if errors.Is(r.Err, errTableDeleted) {
		g.markDeletedTable(keyspaceTableForRanges(r.Ranges))
		r.Err = nil
	}

	if r.Err != nil {
		g.failed += len(r.Ranges)
		g.logger.Info(ctx, "Repair failed", "error", r.Err)
		if g.failFast {
			// If worker failed with fail fast error then initiate shutdown.
			// Setting nextClosed to true will prevent scheduling any new
			// jobs but will also allow draining queue of any existing jobs.
			// generator/worker pair will shutdown once there are no more
			// busy replicas.
			g.closeNext()
		}
	} else {
		g.success += len(r.Ranges)
	}

	if percent := 100 * (g.success + g.failed) / g.count; percent > g.lastPercent {
		g.logger.Info(ctx, "Progress", "percent", percent, "count", g.count, "success", g.success, "failed", g.failed)
		g.lastPercent = percent
	}

	g.unblockReplicas(r.Ranges[0])
}

func (g *generator) unblockReplicas(ttr *tableTokenRange) {
	g.busy.Remove(ttr.Replicas...)
	g.inflightJobs--
}

func (g *generator) closeNext() {
	if !g.nextClosed {
		g.nextClosed = true
		close(g.next)
	}
}

func (g *generator) fillNext(ctx context.Context) {
	if g.nextClosed {
		return
	}
	for {
		hash := g.pickReplicas()
		if hash == 0 {
			return
		}

		var (
			host                       = g.pickHost(hash)
			rangesLimit, shardsPercent = g.rangesLimit(host)
			ranges                     = g.pickRanges(hash, rangesLimit)

			j = job{
				Host:          host,
				Ranges:        ranges,
				ShardsPercent: shardsPercent,
			}
		)

		// Process deleted table as a success without sending to worker
		if k, t := keyspaceTableForRanges(ranges); g.deletedTable(k, t) {
			g.logger.Debug(ctx, "Repair skipping deleted table",
				"keyspace", k,
				"table", t,
				"hosts", ranges[0].Replicas,
				"ranges", len(ranges),
			)
			r := jobResult{job: j}
			g.processResult(ctx, r)
			if err := g.progress.OnJobResult(ctx, r); err != nil {
				g.logger.Error(ctx, "Updating deleted table progress", "host", j.Host, "error", err)
			}
			continue
		}

		// Send job to worker
		select {
		case g.next <- j:
		default:
			panic("next buffer full")
		}
	}
}

// pickReplicas blocks replicas and returns hash, if no replicas can be found
// then 0 is returned.
func (g *generator) pickReplicas() uint64 {
	var (
		stop = g.pos
		pos  = g.pos
	)

	for {
		pos = (pos + 1) % len(g.keys)
		hash := g.keys[pos]

		if len(g.ranges[hash]) > 0 {
			replicas := g.replicas[hash]
			if g.canScheduleRepair(replicas) {
				g.busy.Add(replicas...)
				g.inflightJobs++
				g.pos = pos
				return hash
			}
		}

		if pos == stop {
			return 0
		}
	}
}

func (g *generator) canScheduleRepair(hosts []string) bool {
	// Always make some progress.
	if g.busy.IsEmpty() {
		return true
	}
	// Repair parallel might limit inflight jobs.
	// Check if adding these hosts to busy pool will not reach the limit.
	if g.inflightJobs >= g.parallelLimit() {
		return false
	}
	// Schedule only if hosts aren't busy at the moment.
	return !g.busy.HasAny(hosts...)
}

func (g *generator) pickRanges(hash uint64, limit int) []*tableTokenRange {
	ranges := g.ranges[hash]

	// Speedup repair of system and small tables by repairing all ranges together.
	keyspace, table := keyspaceTableForRanges(ranges)
	if strings.HasPrefix(keyspace, "system") || g.smallTable(keyspace, table) {
		limit = len(ranges)
	}

	var i int
	for i = 0; i < limit; i++ {
		if len(ranges) <= i {
			break
		}
		if i > 0 {
			if ranges[i-1].Keyspace != ranges[i].Keyspace || ranges[i-1].Table != ranges[i].Table {
				break
			}
		}
	}

	g.ranges[hash] = ranges[i:]
	return ranges[0:i]
}

func keyspaceTableForRanges(ranges []*tableTokenRange) (keyspace, table string) {
	return ranges[0].Keyspace, ranges[0].Table
}

func (g *generator) markSmallTable(keyspace, table string) {
	g.smallTables.Add(keyspace + "." + table)
}

func (g *generator) smallTable(keyspace, table string) bool {
	return g.smallTables.Has(keyspace + "." + table)
}

func (g *generator) markDeletedTable(keyspace, table string) {
	g.deletedTables.Add(keyspace + "." + table)
}

func (g *generator) deletedTable(keyspace, table string) bool {
	return g.deletedTables.Has(keyspace + "." + table)
}

func (g *generator) rangesLimit(host string) (ranges int, shardsPercent float64) {
	i := g.intensityHandler.Intensity()

	switch {
	case i == maxIntensity:
		return g.hostRangesLimits[host].Max, 0
	case i < 1:
		return g.hostRangesLimits[host].Default, i
	default:
		return int(i) * g.hostRangesLimits[host].Default, 0
	}
}

func (g *generator) pickHost(hash uint64) string {
	return g.hostPriority.PickHost(g.replicas[hash])
}

func (g *generator) parallelLimit() int {
	p := g.intensityHandler.Parallel()

	if p == defaultParallel || p > g.intensityHandler.MaxParallel() {
		return g.intensityHandler.MaxParallel()
	}

	return p
}
