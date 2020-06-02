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

type hostRangesLimit map[string]int

type job struct {
	Host   string
	Ranges []*tableTokenRange
}

type jobResult struct {
	job
	Err error
}

type generator struct {
	target                  Target
	gracefulShutdownTimeout time.Duration
	logger                  log.Logger

	replicas        map[uint64][]string
	ranges          map[uint64][]*tableTokenRange
	hostPriority    hostPriority
	hostRangesLimit hostRangesLimit

	keys       []uint64
	pos        int
	busy       *strset.Set
	next       chan job
	nextClosed bool
	result     chan jobResult

	count   int
	success int
	failed  int
}

func newGenerator(target Target, gracefulShutdownTimeout time.Duration, logger log.Logger) *generator {
	return &generator{
		target:                  target,
		gracefulShutdownTimeout: gracefulShutdownTimeout,
		logger:                  logger,
		replicas:                make(map[uint64][]string),
		ranges:                  make(map[uint64][]*tableTokenRange),
	}
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
	g.count++
}

func (g *generator) Hosts() *strset.Set {
	all := strset.New()
	for _, v := range g.replicas {
		all.Add(v...)
	}
	return all
}

func (g *generator) SetHostPriority(hp hostPriority) {
	hosts := g.Hosts()

	for _, h := range hosts.List() {
		if _, ok := hp[h]; !ok {
			panic("invalid host priority, missing host")
		}
	}

	g.hostPriority = hp
}

func (g *generator) SetHostRangeLimits(hrl hostRangesLimit) {
	hosts := g.Hosts()

	for _, h := range hosts.List() {
		if _, ok := hrl[h]; !ok {
			panic("invalid host range limits, missing host")
		}
	}

	g.hostRangesLimit = hrl
}

func (g *generator) Init(workerCount int) {
	if len(g.replicas) == 0 {
		panic("cannot init generator, no ranges")
	}
	g.keys = make([]uint64, 0, len(g.replicas))
	for k := range g.replicas {
		g.keys = append(g.keys, k)
	}
	g.pos = rand.Intn(len(g.keys))
	g.busy = strset.New()
	g.next = make(chan job, 2*workerCount)
	g.result = make(chan jobResult)

	g.fillNext()
}

func (g *generator) Next() <-chan job {
	return g.next
}

func (g *generator) Result() chan<- jobResult {
	return g.result
}

func (g *generator) Run(ctx context.Context) error {
	g.logger.Info(ctx, "Start repair")

	//TODO: progress and state registration
	lastPercent := -1

	done := ctx.Done()
	stop := make(chan struct{})
loop:
	for {
		select {
		case <-stop:
			break loop
		case <-done:
			g.logger.Info(ctx, "Graceful repair shutdown", "timeout", g.gracefulShutdownTimeout)
			// Stop workers by closing next channel
			g.closeNext()

			done = nil
			time.AfterFunc(g.gracefulShutdownTimeout, func() {
				close(stop)
			})
		case r := <-g.result:
			// TODO progress and state registration
			// TODO handling penalties
			lastPercent = g.processResult(ctx, r, lastPercent)
			g.fillNext()

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
	return nil
}

func (g *generator) processResult(ctx context.Context, r jobResult, lastPercent int) int {
	if r.Err != nil {
		g.failed += len(r.Ranges)
		g.logger.Info(ctx, "Repair failed", "error", r.Err)
	} else {
		g.success += len(r.Ranges)
	}

	if percent := 100 * (g.success + g.failed) / g.count; percent > lastPercent {
		g.logger.Info(ctx, "Progress", "percent", percent, "count", g.count, "success", g.success, "failed", g.failed)
		lastPercent = percent
	}

	g.unblockReplicas(r.Ranges[0])

	return lastPercent
}

func (g *generator) unblockReplicas(ttr *tableTokenRange) {
	g.busy.Remove(ttr.Replicas...)
}

func (g *generator) closeNext() {
	if !g.nextClosed {
		close(g.next)
		g.nextClosed = true
	}
}

func (g *generator) fillNext() {
	if g.nextClosed {
		return
	}
	for {
		hash := g.pickReplicas()
		if hash == 0 {
			return
		}

		host := g.pickHost(hash)
		rangesLimit := g.rangesLimit(host)

		select {
		case g.next <- job{
			Host:   host,
			Ranges: g.pickRanges(hash, rangesLimit),
		}:
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
			if items := g.replicas[hash]; !g.busy.HasAny(items...) {
				g.busy.Add(items...)
				g.pos = pos
				return hash
			}
		}

		if pos == stop {
			return 0
		}
	}
}

func (g *generator) pickRanges(hash uint64, limit int) []*tableTokenRange {
	ranges := g.ranges[hash]

	// Speedup repair of system tables be repairing all ranges together.
	if strings.HasPrefix(ranges[0].Keyspace, "system") {
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

func (g *generator) rangesLimit(host string) int {
	limit := g.hostRangesLimit[host]
	if g.target.Intensity != 0 {
		limit = g.target.Intensity
	}
	if limit == 0 {
		limit = 1
	}
	return limit
}

func (g *generator) pickHost(hash uint64) string {
	return g.hostPriority.PickHost(g.replicas[hash])
}
