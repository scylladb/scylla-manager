// Copyright (C) 2017 ScyllaDB

package repair

import (
	"context"
	"math/rand"
	"strings"

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
	return ""
}

type job struct {
	Host   string
	Ranges []*tableTokenRange
}

type jobResult struct {
	job
	Err error
}

type generator struct {
	target Target
	logger log.Logger

	replicas     map[uint64][]string
	ranges       map[uint64][]*tableTokenRange
	hostPriority hostPriority

	keys   []uint64
	pos    int
	busy   *strset.Set
	next   chan job
	result chan jobResult

	count   int
	success int
	failed  int
}

func newGenerator(target Target, logger log.Logger) *generator {
	return &generator{
		target:   target,
		logger:   logger,
		replicas: make(map[uint64][]string),
		ranges:   make(map[uint64][]*tableTokenRange),
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

	lastPercent := -1

	for {
		select {
		case <-ctx.Done():
			// TODO graceful stop and wait for worker results at this point
			// g.next should be closed and we just collect results
			return ctx.Err()
		case r := <-g.result:
			// TODO progress and state registration
			// TODO handling penalties
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
			g.fillNext()

			if done := g.busy.IsEmpty(); done {
				g.logger.Info(ctx, "Done repair")

				close(g.next)

				if g.failed > 0 {
					return errors.Errorf("%d token ranges out of %d failed to repair", g.failed, g.count)
				}
				return nil
			}
		}
	}
}

func (g *generator) unblockReplicas(ttr *tableTokenRange) {
	g.busy.Remove(ttr.Replicas...)
}

func (g *generator) fillNext() {
	for {
		hash := g.pickReplicas()
		if hash == 0 {
			return
		}

		select {
		case g.next <- job{
			Host:   g.pickHost(hash),
			Ranges: g.pickRanges(hash),
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

func (g *generator) pickRanges(hash uint64) []*tableTokenRange {
	ranges := g.ranges[hash]

	nrRanges := g.target.Intensity
	if nrRanges == 0 {
		nrRanges = 1
	}

	// Speedup repair of system tables be repairing all ranges together.
	if strings.HasPrefix(ranges[0].Keyspace, "system") {
		nrRanges = len(ranges)
	}

	var i int
	for i = 0; i < nrRanges; i++ {
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

func (g *generator) pickHost(hash uint64) string {
	return g.hostPriority.PickHost(g.replicas[hash])
}
