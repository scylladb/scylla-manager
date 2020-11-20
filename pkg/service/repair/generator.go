// Copyright (C) 2017 ScyllaDB

package repair

import (
	"context"
	"strings"
	"time"

	"github.com/pkg/errors"
	"github.com/scylladb/go-log"
	"github.com/scylladb/go-set/strset"
)

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

// allowance specifies the amount of work a worker can do in a job.
type allowance struct {
	Replicas      []string
	Ranges        int
	ShardsPercent float64
}

var nilAllowance = allowance{}

type job struct {
	Host      string
	Allowance allowance
	Ranges    []*tableTokenRange
}

type jobResult struct {
	job
	Err error
}

type generator struct {
	gracefulStopTimeout time.Duration
	progress            progressManager
	logger              log.Logger

	failFast bool

	replicas      map[uint64][]string
	replicasIndex map[uint64]int
	ranges        map[uint64][]*tableTokenRange
	hostCount     int
	smallTables   *strset.Set
	deletedTables *strset.Set
	lastPercent   int

	ctl          controller
	hostPriority hostPriority

	keys       []uint64
	pos        int
	next       chan job
	nextClosed bool
	result     chan jobResult

	count   int
	success int
	failed  int
}

type generatorOption func(*generator)

func failFast(g *generator) {
	g.failFast = true
}

func newGenerator(gracefulStopTimeout time.Duration, progress progressManager, logger log.Logger) *generator {
	g := &generator{
		gracefulStopTimeout: gracefulStopTimeout,
		progress:            progress,
		logger:              logger,

		replicas:      make(map[uint64][]string),
		replicasIndex: make(map[uint64]int),
		ranges:        make(map[uint64][]*tableTokenRange),
		smallTables:   strset.New(),
		deletedTables: strset.New(),
		lastPercent:   -1,
	}

	return g
}

func (g *generator) Add(ctx context.Context, ranges []*tableTokenRange) {
	for _, ttr := range ranges {
		g.add(ctx, ttr)
	}
}

func (g *generator) add(ctx context.Context, ttr *tableTokenRange) {
	hash := ttr.ReplicaHash()

	if len(ttr.Replicas) == 0 {
		// This is handled in the builder and should never happen.
		// It is logged just in case - so that we are aware.
		g.logger.Error(ctx, "Trying to add not replicated token range, ignoring", "value", ttr)
		return
	}

	if _, ok := g.replicas[hash]; !ok {
		g.replicas[hash] = ttr.Replicas
		g.replicasIndex[hash] = len(g.replicasIndex)
	}
	g.ranges[hash] = append(g.ranges[hash], ttr)
}

func (g *generator) Hosts() *strset.Set {
	all := strset.New()
	for _, v := range g.replicas {
		all.Add(v...)
	}
	return all
}

func (g *generator) Size() int {
	return len(g.replicas)
}

func (g *generator) Init(ctx context.Context, ctl controller, hp hostPriority, opts ...generatorOption) error {
	for _, o := range opts {
		o(g)
	}

	g.ctl = ctl
	g.hostPriority = hp

	if g.Size() == 0 {
		return errors.New("no replicas to repair")
	}

	// Preserve replicas order in the ring. This improves parallel behaviour,
	// if we pick replicas randomly we increase repair fragmentation.
	// Say we have 6 nodes [a, b, c, d, e, f] and RF=2 if, when we block replicas
	// [a,b] and [d,e] we cannot repair any more replicas in parallel even when
	// it's possible in theory to have 3 repairs in parallel.
	g.keys = make([]uint64, len(g.replicas))
	for hash, i := range g.replicasIndex {
		g.keys[i] = hash
	}
	g.hostCount = g.Hosts().Size()

	g.next = make(chan job, ctl.MaxWorkerCount())
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
			g.processResult(ctx, r)
			g.fillNext(ctx)

			// At this point if there are no blocked replicas it means no worker
			// will process anything and report results ever, we should stop now.
			if !g.ctl.Busy() {
				g.logger.Info(ctx, "Done repair")
				g.closeNext()
				break loop
			}
		}
	}

	if g.failed > 0 {
		return errors.Errorf("repair %d token ranges out of %d", g.failed, g.count)
	}
	return err
}

func (g *generator) processResult(ctx context.Context, r jobResult) {
	if errors.Is(r.Err, errTableDeleted) {
		g.markDeletedTable(keyspaceTableForRanges(r.Ranges))
		r.Err = nil
	}

	n := len(r.Ranges)

	if r.Err != nil {
		g.failed += n
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
		g.success += n
	}

	if percent := 100 * (g.success + g.failed) / g.count; percent > g.lastPercent {
		g.logger.Info(ctx, "Progress", "percent", percent, "count", g.count, "success", g.success, "failed", g.failed)
		g.lastPercent = percent
	}

	g.ctl.Unblock(r.Allowance)
}

func (g *generator) closeNext() {
	if !g.nextClosed {
		g.nextClosed = true
		close(g.next)
	}
}

func (g *generator) fillNext(ctx context.Context) {
	if ctx.Err() != nil || g.nextClosed {
		return
	}
	for {
		hash, allowance := g.pickReplicas()
		if hash == 0 {
			return
		}

		j := job{
			Host:      g.pickHost(hash),
			Allowance: allowance,
			Ranges:    g.pickRanges(hash, allowance.Ranges),
		}

		// Process deleted table as a success without sending to worker
		if k, t := keyspaceTableForRanges(j.Ranges); g.deletedTable(k, t) {
			g.logger.Debug(ctx, "Repair skipping deleted table",
				"keyspace", k,
				"table", t,
				"hosts", j.Ranges[0].Replicas,
				"ranges", len(j.Ranges),
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
func (g *generator) pickReplicas() (uint64, allowance) {
	var (
		stop = g.pos
		pos  = g.pos
	)

	for {
		pos = (pos + 1) % len(g.keys)
		hash := g.keys[pos]

		if len(g.ranges[hash]) > 0 {
			ok, a := g.ctl.TryBlock(g.replicas[hash])
			if ok {
				return hash, a
			}
		}

		if pos == stop {
			return 0, nilAllowance
		}
	}
}

func (g *generator) pickRanges(hash uint64, limit int) []*tableTokenRange {
	ranges := g.ranges[hash]

	// Speedup repair of small tables by repairing all ranges together.
	keyspace, table := keyspaceTableForRanges(ranges)
	if strings.HasPrefix(keyspace, "system") || g.smallTable(keyspace, table) || g.deletedTable(keyspace, table) {
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

func (g *generator) pickHost(hash uint64) string {
	return g.hostPriority.PickHost(g.replicas[hash])
}
