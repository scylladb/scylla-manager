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

func (w fakeWorker) tryTakeJob() (job, bool) {
	select {
	case job, ok := <-w.In:
		return job, ok
	default:
	}

	return job{}, false
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

	rangeLimits := hostRangesLimit{
		"a": 5,
		"b": 10,
		"c": 20,
		"d": 5,
		"e": 10,
		"f": 20,
	}

	units := []Unit{
		{Keyspace: "kn0", Tables: []string{"tn0", "tn1"}},
		{Keyspace: "kn1", Tables: []string{"tn0", "tn1"}},
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

	ih := newIntensityHandler()

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

		ctx := context.Background()
		target := Target{
			DC:        []string{"dc1", "dc2"},
			Intensity: 50,
		}
		b := newTableTokenRangeBuilder(target, hostDC).Add(ranges)
		g := newGenerator(ih, gracefulShutdownTimeout, log.NewDevelopment(), newNopProgressManager())

		var allRanges []*tableTokenRange
		for _, u := range units {
			allRanges = append(allRanges, b.Build(u)...)
			g.Add(b.Build(u))
		}

		g.SetHostPriority(hostPriority)
		if err := g.Init(ctx, workerCount(ranges)); err != nil {
			t.Fatal(err)
		}
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

	t.Run("Host ranges limit", func(t *testing.T) {
		var ranges []scyllaclient.TokenRange
		for i := 0; i < 100; i += 4 {
			ranges = append(ranges, []scyllaclient.TokenRange{
				{
					StartToken: int64(i),
					EndToken:   int64(i + 1),
					Replicas:   []string{"a", "b"},
				},
				{
					StartToken: int64(i + 2),
					EndToken:   int64(i + 3),
					Replicas:   []string{"b", "c"},
				},
			}...)
		}

		t.Run("ranges are distributed within host limit", func(t *testing.T) {
			target := Target{
				DC: []string{"dc1", "dc2"},
			}
			ctx := context.Background()
			g := makeGenerator(ctx, target, ih, units, hostDC, ranges, hostPriority, rangeLimits)
			go g.Run(ctx)

			w := fakeWorker{
				In:     g.Next(),
				Out:    g.Result(),
				Logger: log.NewDevelopment(),
			}
			jobs := w.drainJobs(ctx)

			// Check that ranges follow host limit
			for _, j := range jobs {
				if len(j.Ranges) > rangeLimits[j.Host] {
					t.Errorf("%s host received more ranges than can handle", j.Host)
				}
			}
		})

		t.Run("ranges are distributed within intensity limit", func(t *testing.T) {
			target := Target{
				DC:        []string{"dc1", "dc2"},
				Intensity: 10,
			}

			ctx := context.Background()
			g := makeGenerator(ctx, target, ih, units, hostDC, ranges, hostPriority, rangeLimits)
			go g.Run(ctx)

			w := fakeWorker{
				In:     g.Next(),
				Out:    g.Result(),
				Logger: log.NewDevelopment(),
			}
			jobs := w.drainJobs(ctx)

			// Check that ranges follow host limit
			for _, j := range jobs {
				if len(j.Ranges) > int(target.Intensity) {
					t.Errorf("%s host received more ranges than intensity", j.Host)
				}
			}
		})

		t.Run("number of active hosts is limited by intensity between (0, 1)", func(t *testing.T) {
			ranges = []scyllaclient.TokenRange{
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

			table := []struct {
				Name                  string
				Intensity             float64
				ExpectedActiveWorkers int
			}{
				{
					Name:                  "One third",
					Intensity:             0.333,
					ExpectedActiveWorkers: 1,
				},
				{
					Name:                  "Two thirds",
					Intensity:             0.666,
					ExpectedActiveWorkers: 2,
				},
				{
					Name:                  "Three thirds",
					Intensity:             0.999,
					ExpectedActiveWorkers: 3,
				},
				{
					Name:                  "Intensity close to 0 does not stop progress",
					Intensity:             0.001,
					ExpectedActiveWorkers: 1,
				},
			}

			for i := range table {
				test := table[i]
				t.Run(test.Name, func(t *testing.T) {
					ih = newIntensityHandler()
					target := Target{
						DC:        []string{"dc1", "dc2"},
						Intensity: test.Intensity,
					}
					ctx := context.Background()
					g := makeGenerator(ctx, target, ih, units, hostDC, ranges, hostPriority, rangeLimits)

					generatorStarted := make(chan struct{})
					go func() {
						close(generatorStarted)
						g.Run(ctx)
					}()
					<-generatorStarted

					workers := make([]fakeWorker, workerCount(ranges))
					for i := range workers {
						workers[i] = fakeWorker{
							In:     g.Next(),
							Out:    g.Result(),
							Logger: log.NewDevelopment(),
						}
					}

					var jobs []job
					for _, w := range workers {
						j, ok := w.tryTakeJob()
						if ok {
							jobs = append(jobs, j)
						}
					}

					activeWorkers := float64(len(jobs))
					if a, b := epsilonRange(float64(test.ExpectedActiveWorkers)); activeWorkers < a || activeWorkers > b {
						t.Errorf("number of active workers differs from limit, expected %d, got %.0f ", test.ExpectedActiveWorkers, activeWorkers)
					}

					for _, j := range jobs {
						if len(j.Ranges) != 1 {
							t.Errorf("workers repair more than 1 range at a time")
						}
					}
				})
			}
		})
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

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		g := makeGenerator(ctx, target, ih, units, hostDC, ranges, hostPriority, rangeLimits)

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

		wc := workerCount(ranges)
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

	t.Run("Change intensity in flight", func(t *testing.T) {
		Print("Given: at least 3 ranges to repair")
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
				Replicas:   []string{"a", "b"},
			},
		}

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		Print("Given: intensity of 1")
		ih = newIntensityHandler()
		intensity := float64(1)

		target := Target{
			DC:        []string{"dc1"},
			Intensity: intensity,
		}

		g := makeGenerator(ctx, target, ih, units, hostDC, ranges, hostPriority, rangeLimits)

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

		Print("Given: worker")
		w := fakeWorker{
			In:     g.Next(),
			Out:    g.Result(),
			Logger: log.NewDevelopment().Named("worker"),
		}

		Print("When: worker get his job")
		j, ok := w.takeJob()
		if !ok {
			t.Error("worker couldn't take job")
		}

		Print("Then: worker get at most 1 range at once")
		if len(j.Ranges) != int(intensity) {
			t.Errorf("worker intensity is wrong, got %d, expected %f", len(j.Ranges), intensity)
		}

		Print("When: intensity is changed to 2")
		intensity = 2
		if err := ih.Set(ctx, intensity); err != nil {
			t.Fatal(err)
		}

		Print("When: worker finishes first and take a new job")
		w.execute(j)
		j, ok = w.takeJob()
		if !ok {
			t.Error("worker couldn't take job")
		}

		Print("Then: worker number of ranges at once was increased according to intensity")
		if len(j.Ranges) != int(intensity) {
			t.Errorf("worker intensity is wrong, got %d, expected %f", len(j.Ranges), intensity)
		}
	})
}

func makeGenerator(ctx context.Context, target Target, intensityHandler *intensityHandler, units []Unit, hostDC map[string]string, ranges []scyllaclient.TokenRange, hostPriority hostPriority, rangeLimits hostRangesLimit) *generator {
	b := newTableTokenRangeBuilder(target, hostDC).Add(ranges)

	if err := intensityHandler.Set(ctx, target.Intensity); err != nil {
		panic(err)
	}
	intensityHandler.SetHostRangeLimits(rangeLimits)

	g := newGenerator(intensityHandler, gracefulShutdownTimeout, log.NewDevelopment(), newNopProgressManager())
	for _, u := range units {
		g.Add(b.Build(u))
	}

	g.SetHostPriority(hostPriority)

	if err := g.Init(ctx, workerCount(ranges)); err != nil {
		panic(err)
	}
	return g
}

func epsilonRange(v float64) (float64, float64) {
	e := v * 1.05
	return v - e, v + e
}

func newIntensityHandler() *intensityHandler {
	return &intensityHandler{
		c:      make(chan float64, 1),
		global: atomic.NewFloat64(0),
	}
}
