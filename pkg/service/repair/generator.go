// Copyright (C) 2017 ScyllaDB

package repair

import (
	"context"
	"math"
	"sort"
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

	// Order in which tables should be repaired.
	repairOrder []TableName
	// Table's index in repairOrder.
	tableIdx map[TableName]int
	// remainingRanges[host][ord] = cnt ->
	// host has cnt remaining ranges to repair for table repairOrder[ord].
	remainingRanges map[string][]int
	// The index (in repairOrder) of currently repaired table.
	// Even though some other tables might be repaired in parallel, this table has full priority,
	// so it won't be blocked by any other repairs.
	currentTableIdx int

	replicas      map[uint64][]string
	replicasIndex map[uint64]int
	ranges        map[uint64][]*tableTokenRange
	minRf         int
	smallTables   *strset.Set
	deletedTables *strset.Set
	lastPercent   int

	ctl          controller
	hostPriority hostPriority

	keys         []uint64
	posCurrTable int
	posAnyTable  int

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

		tableIdx:        make(map[TableName]int),
		remainingRanges: make(map[string][]int),
		replicas:        make(map[uint64][]string),
		replicasIndex:   make(map[uint64]int),
		ranges:          make(map[uint64][]*tableTokenRange),
		minRf:           math.MaxInt8,
		smallTables:     strset.New(),
		deletedTables:   strset.New(),
		lastPercent:     -1,
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

	if l := len(ttr.Replicas); l < g.minRf {
		g.minRf = l
	}
}

func (g *generator) Hosts() *strset.Set {
	all := strset.New()
	for _, v := range g.replicas {
		all.Add(v...)
	}
	return all
}

func (g *generator) MinReplicationFactor() int {
	return g.minRf
}

func (g *generator) Size() int {
	return len(g.replicas)
}

func (g *generator) Init(ctx context.Context, ctl controller, hp hostPriority, repairOrder []TableName, opts ...generatorOption) error {
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

	g.next = make(chan job, ctl.MaxWorkerCount())
	g.result = make(chan jobResult)

	var trs []*tableTokenRange
	for _, ttrs := range g.ranges {
		trs = append(trs, ttrs...)
	}
	if err := g.progress.Init(ctx, trs); err != nil {
		return err
	}

	// Remove repaired ranges from the pool of available ones to avoid their scheduling
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

	g.repairOrder = repairOrder
	for i, t := range g.repairOrder {
		g.tableIdx[t] = i
	}

	// Sort tableTokenRanges in repairOrder for each replica set
	for _, r := range g.ranges {
		sort.Slice(r, func(i, j int) bool {
			tI := TableName{Keyspace: r[i].Keyspace, Table: r[i].Table}
			tJ := TableName{Keyspace: r[j].Keyspace, Table: r[j].Table}
			// Sort by repairOrder
			ordI := g.tableIdx[tI]
			ordJ := g.tableIdx[tJ]
			if ordI != ordJ {
				return ordI < ordJ
			}
			// Sort by tokens
			return r[i].StartToken < r[j].StartToken
		})
	}

	// Calculate remainingRanges
	for _, r := range g.ranges {
		for _, ttr := range r {
			ord := g.tableIdx[TableName{
				Keyspace: ttr.Keyspace,
				Table:    ttr.Table,
			}]

			for _, h := range ttr.Replicas {
				if g.remainingRanges[h] == nil {
					g.remainingRanges[h] = make([]int, len(g.repairOrder))
				}
				g.remainingRanges[h][ord]++
			}
		}
	}
	g.moveCurrentTableIdx()

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

	gracefulShutdown := func() {
		g.logger.Info(ctx, "Graceful repair shutdown", "timeout", g.gracefulStopTimeout)
		// Stop workers by closing next channel
		g.closeNext()

		done = nil
		time.AfterFunc(g.gracefulStopTimeout, func() {
			close(stop)
		})
		err = ctx.Err()
	}
loop:
	for {
		select {
		case <-done:
			gracefulShutdown()
		default:
		}

		select {
		case <-done:
			gracefulShutdown()
		case <-stop:
			break loop
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
		g.markDeletedTable(tableForRanges(r.Ranges))
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
			// generator/worker pair will shut down once there are no more
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

	reps := replicasForRanges(r.Ranges)
	ord := g.tableIdx[tableForRanges(r.Ranges)]

	for _, h := range reps {
		g.remainingRanges[h][ord] -= n
	}
	g.moveCurrentTableIdx()

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
	g.posCurrTable = 0
	g.posAnyTable = 0
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
		if t := tableForRanges(j.Ranges); g.deletedTable(t) {
			g.logger.Debug(ctx, "Repair skipping deleted table",
				"keyspace", t.Keyspace,
				"table", t.Table,
				"hosts", j.Ranges[0].Replicas,
				"ranges", len(j.Ranges),
			)
			r := jobResult{job: j}
			g.processResult(ctx, r)
			g.progress.OnJobResult(ctx, r)
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
	// First iteration returns replicas with jobs for currently repaired table
	for ; g.posCurrTable < len(g.keys); g.posCurrTable++ {
		hash := g.keys[g.posCurrTable]

		if len(g.ranges[hash]) > 0 {
			t := tableForRanges(g.ranges[hash])
			if t != g.repairOrder[g.currentTableIdx] {
				continue
			}

			if ok, a := g.ctl.TryBlock(g.replicas[hash]); ok {
				return hash, a
			}
		}
	}

	// Second iteration returns replicas with jobs non-conflicting with repairing current table
	for ; g.posAnyTable < len(g.keys); g.posAnyTable++ {
		hash := g.keys[g.posAnyTable]

		if len(g.ranges[hash]) > 0 {
			ok, a := g.ctl.TryBlock(g.replicas[hash])
			if !ok {
				continue
			}

			reps := g.replicas[hash]
			ord := g.tableIdx[tableForRanges(g.ranges[hash])]
			for _, h := range reps {
				for i := g.currentTableIdx; i < ord; i++ {
					// Deny job which requires host that still has jobs
					// for tables with lower repairOrder index.
					if g.remainingRanges[h][i] > 0 {
						ok = false
						break
					}
				}
				if !ok {
					break
				}
			}

			if ok {
				return hash, a
			}
			g.ctl.Unblock(a)
		}
	}

	return 0, nilAllowance
}

func (g *generator) pickRanges(hash uint64, limit int) []*tableTokenRange {
	ranges := g.ranges[hash]

	// Speedup repair of small tables by repairing all ranges together.
	t := tableForRanges(ranges)
	if strings.HasPrefix(t.Keyspace, "system") || g.smallTable(t) || g.deletedTable(t) {
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

func tableForRanges(ranges []*tableTokenRange) TableName {
	return TableName{
		Keyspace: ranges[0].Keyspace,
		Table:    ranges[0].Table,
	}
}

func replicasForRanges(ranges []*tableTokenRange) []string {
	return ranges[0].Replicas
}

func (g *generator) markSmallTable(table TableName) {
	g.smallTables.Add(table.Keyspace + "." + table.Table)
}

func (g *generator) smallTable(table TableName) bool {
	return g.smallTables.Has(table.Keyspace + "." + table.Table)
}

func (g *generator) markDeletedTable(table TableName) {
	g.deletedTables.Add(table.Keyspace + "." + table.Table)
}

func (g *generator) deletedTable(table TableName) bool {
	return g.deletedTables.Has(table.Keyspace + "." + table.Table)
}

func (g *generator) pickHost(hash uint64) string {
	return g.hostPriority.PickHost(g.replicas[hash])
}

// moveCurrentTableIdx increases currentTableIdx so that it points to the next not fully repaired table.
func (g *generator) moveCurrentTableIdx() {
	for ; g.currentTableIdx < len(g.repairOrder); g.currentTableIdx++ {
		for _, rr := range g.remainingRanges {
			if rr[g.currentTableIdx] != 0 {
				return
			}
		}
	}
}
