// Copyright (C) 2017 ScyllaDB

package repair

import (
	"context"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	"github.com/scylladb/go-log"
	"github.com/scylladb/mermaid/pkg/scyllaclient"
	. "github.com/scylladb/mermaid/pkg/testutils"
	"go.uber.org/atomic"
)

const (
	gracefulShutdownTimeout = 5 * time.Second
)

type fakeWorker struct {
	In     <-chan job
	Out    chan<- jobResult
	Logger log.Logger
}

func (w fakeWorker) takeJob() (job, bool) {
	job, ok := <-w.In
	return job, ok
}

func (w fakeWorker) execute(j job) {
	w.Out <- jobResult{
		job: j,
	}
}

func (w fakeWorker) drainJobs(ctx context.Context) (jobs []job) {
	for {
		job, ok := w.takeJob()
		if !ok {
			w.Logger.Info(ctx, "Done")
			return
		}
		w.Logger.Info(ctx, "Rcv", "job", job)
		jobs = append(jobs, job)
		w.execute(job)
	}
}

// TODO add more tests, we must test things like stopping, we should move as
// much tests we can to unit tests.

func TestGenerator(t *testing.T) {
	hostDC := map[string]string{
		"a": "dc1",
		"b": "dc1",
		"c": "dc1",
		"d": "dc2",
		"e": "dc2",
		"f": "dc2",
	}

	hostPriority := hostPriority{
		"a": 1,
		"b": 1,
		"c": 1,
		"d": 2,
		"e": 2,
		"f": 2,
	}

	ttrLess := func(a, b *tableTokenRange) bool {
		if a.Keyspace < b.Keyspace {
			return true
		}
		if a.Keyspace > b.Keyspace {
			return false
		}
		if a.Table < b.Table {
			return true
		}
		if a.Table > b.Table {
			return false
		}
		return a.Pos < b.Pos
	}

	t.Run("Basic", func(t *testing.T) {
		ranges := []scyllaclient.TokenRange{
			{
				StartToken: 1,
				EndToken:   2,
				Replicas:   []string{"a", "b"},
			},
			{
				StartToken: 3,
				EndToken:   4,
				Replicas:   []string{"a", "b"},
			},
			{
				StartToken: 5,
				EndToken:   6,
				Replicas:   []string{"c", "d"},
			},
			{
				StartToken: 7,
				EndToken:   8,
				Replicas:   []string{"e", "f"},
			},
		}

		target := Target{
			DC: []string{"dc1", "dc2"},
		}
		b := newTableTokenRangeBuilder(target, hostDC).Add(ranges)
		g := newGenerator(target, gracefulShutdownTimeout, log.NewDevelopment())

		var allRanges []*tableTokenRange
		for _, u := range []Unit{{Keyspace: "kn0", Tables: []string{"tn0", "tn1"}}, {Keyspace: "kn1", Tables: []string{"tn0", "tn1"}}} {
			allRanges = append(allRanges, b.Build(u)...)
			g.Add(b.Build(u))
		}

		g.SetHostPriority(hostPriority)
		g.Init(workerCount(ranges))
		ctx := context.Background()
		go g.Run(ctx)

		w := fakeWorker{
			In:     g.Next(),
			Out:    g.Result(),
			Logger: log.NewDevelopment(),
		}
		jobs := w.drainJobs(ctx)

		// Check that all ranges are covered
		var drainedRanges []*tableTokenRange
		for _, j := range jobs {
			drainedRanges = append(drainedRanges, j.Ranges...)
		}
		if diff := cmp.Diff(drainedRanges, allRanges, cmpopts.SortSlices(ttrLess)); diff != "" {
			t.Error("Ranges mismatch diff", diff)
		}
	})

	t.Run("Graceful shutdown", func(t *testing.T) {
		ranges := []scyllaclient.TokenRange{
			{
				StartToken: 1,
				EndToken:   2,
				Replicas:   []string{"a", "b"},
			},
			{
				StartToken: 3,
				EndToken:   4,
				Replicas:   []string{"c", "d"},
			},
			{
				StartToken: 5,
				EndToken:   6,
				Replicas:   []string{"e", "f"},
			},
		}

		target := Target{
			DC: []string{"dc1", "dc2"},
		}
		b := newTableTokenRangeBuilder(target, hostDC).Add(ranges)
		g := newGenerator(target, gracefulShutdownTimeout, log.NewDevelopment())

		var allRanges []*tableTokenRange
		for _, u := range []Unit{{Keyspace: "kn0", Tables: []string{"tn0", "tn1"}}, {Keyspace: "kn1", Tables: []string{"tn0", "tn1"}}} {
			allRanges = append(allRanges, b.Build(u)...)
			g.Add(b.Build(u))
		}

		g.SetHostPriority(hostPriority)

		wc := workerCount(ranges)
		g.Init(wc)
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		Print("Given: running generator")
		generatorFinished := atomic.NewBool(false)
		generatorStarted := make(chan struct{})
		go func() {
			close(generatorStarted)
			g.Run(ctx)
			generatorFinished.Store(true)
		}()

		// Wait for generator to start
		<-generatorStarted

		Print("Given: multiple workers")
		workers := make([]fakeWorker, wc)
		for i := 0; i < wc; i++ {
			workers[i] = fakeWorker{
				In:     g.Next(),
				Out:    g.Result(),
				Logger: log.NewDevelopment().With("worker", i),
			}
		}

		Print("When: all workers starts repairing")
		jobs := make([]job, 0, len(workers))
		for _, w := range workers {
			j, ok := w.takeJob()
			if !ok {
				t.Error("worker couldn't take job, generator finished too fast")
			}
			jobs = append(jobs, j)
		}

		Print("When: repair is interrupted")
		cancel()

		Print("Then: generator is gracefully shutting down")
		if generatorFinished.Load() {
			t.Error("Generator finished without waiting for workers to finish")
		}

		_, ok := workers[0].takeJob()
		if ok {
			t.Error("Generator didn't close jobs channel")
		}

		Print("When: all but one workers finished")
		for i, w := range workers {
			if i != 0 {
				w.execute(jobs[i])
			}
		}

		Print("Then: generator is still waiting for the last worker")
		if generatorFinished.Load() {
			t.Error("Generator finished without waiting for workers to finish")
		}

		Print("When: last worker finish his job")
		workers[0].execute(jobs[0])

		Print("Then: generator also finishes")

		WaitCond(t, func() bool {
			return generatorFinished.Load()
		}, 50*time.Millisecond, 1*time.Second)
	})
}
