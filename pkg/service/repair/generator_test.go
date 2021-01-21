// Copyright (C) 2017 ScyllaDB

package repair

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	"github.com/scylladb/go-log"
	"github.com/scylladb/go-set/u64set"
	"github.com/scylladb/scylla-manager/pkg/scyllaclient"
	. "github.com/scylladb/scylla-manager/pkg/testutils"
	"go.uber.org/atomic"
)

const (
	gracefulStopTimeout = 5 * time.Second
)

type fakeWorker struct {
	Ctx      context.Context
	In       <-chan job
	Out      chan<- jobResult
	Logger   log.Logger
	DumpJobs bool
}

func (w fakeWorker) takeJob() (job, bool) {
	job, ok := <-w.In
	if ok {
		w.logJob(job)
	}
	return job, ok
}

func (w fakeWorker) tryTakeJob() (job, bool) {
	select {
	case job, ok := <-w.In:
		if ok {
			w.logJob(job)
		}
		return job, ok
	default:
		return job{}, false
	}
}

func (w fakeWorker) execute(j job) {
	w.Out <- jobResult{
		job: j,
	}
}

func (w fakeWorker) drainJobs() (jobs []job) {
	for {
		job, ok := w.takeJob()
		if !ok {
			w.Logger.Info(w.Ctx, "Done")
			return
		}
		w.logJob(job)
		jobs = append(jobs, job)
		w.execute(job)
	}
}

func (w fakeWorker) logJob(job job) {
	if w.DumpJobs {
		w.Logger.Info(w.Ctx, "Rcv", "job", job)
	} else {
		w.Logger.Info(w.Ctx, "Rcv",
			"replicas", job.Ranges[0].Replicas,
			"table", job.Ranges[0].Keyspace+"."+job.Ranges[0].Table,
			"ranges", len(job.Ranges),
		)
	}
}

func TestGenerator(t *testing.T) {
	suite := newGeneratorTestSuite()
	t.Run("Basic", suite.Basic)
	t.Run("Intensity", suite.Intensity)
	t.Run("IntensityChange", suite.IntensityChange)
	t.Run("Parallel", suite.Parallel)
	t.Run("SingleDatacenter", suite.SingleDatacenter)
	t.Run("SmallTables", suite.SmallTables)
	t.Run("GracefulShutdown", suite.GracefulShutdown)
}

func TestGeneratorRowLevelRepair(t *testing.T) {
	suite := newGeneratorTestSuite()
	t.Run("Basic", suite.Basic)
	t.Run("Parallel", suite.Parallel)
	t.Run("SingleDatacenter", suite.SingleDatacenter)
	t.Run("SmallTables", suite.SmallTables)
	t.Run("GracefulShutdown", suite.GracefulShutdown)
}

const (
	tables        = 4
	replicaSets   = 6
	tokensInRange = 16
)

type generatorTestSuite struct {
	hostDC           map[string]string
	hostPriority     hostPriority
	hostRangesLimits hostRangesLimit
	dcs              []string
	units            []Unit
	ranges           []scyllaclient.TokenRange

	rowLevelRepair bool
}

func newGeneratorTestSuite() generatorTestSuite {
	suite := generatorTestSuite{}

	suite.hostDC = map[string]string{
		"a": "dc1",
		"b": "dc1",
		"c": "dc1",
		"d": "dc2",
		"e": "dc2",
		"f": "dc2",
	}

	suite.hostPriority = hostPriority{
		"a": 1,
		"b": 1,
		"c": 1,
		"d": 2,
		"e": 2,
		"f": 2,
	}

	suite.hostRangesLimits = hostRangesLimit{
		"a": rangesLimit{Default: 2, Max: 5},
		"b": rangesLimit{Default: 3, Max: 10},
		"c": rangesLimit{Default: 2, Max: 20},
		"d": rangesLimit{Default: 3, Max: 5},
		"e": rangesLimit{Default: 2, Max: 10},
		"f": rangesLimit{Default: 3, Max: 20},
	}

	suite.dcs = []string{"dc1", "dc2"}

	suite.units = []Unit{
		{Keyspace: "kn0", Tables: []string{"tn0", "tn1"}},
		{Keyspace: "kn1", Tables: []string{"tn0", "tn1"}},
	}

	for i := 0; i < 100; i++ {
		var replicas []string
		switch i % replicaSets {
		case 0:
			replicas = []string{"a", "b"}
		case 1:
			replicas = []string{"b", "c"}
		case 2:
			replicas = []string{"c", "d"}
		case 3:
			replicas = []string{"d", "e"}
		case 4:
			replicas = []string{"e", "f"}
		case 5:
			replicas = []string{"f", "a"}
		}

		offset := i * tokensInRange

		suite.ranges = append(suite.ranges,
			scyllaclient.TokenRange{
				StartToken: int64(offset),
				EndToken:   int64(offset + tokensInRange),
				Replicas:   replicas,
			},
		)
	}

	return suite
}

type keyspaceTableName struct {
	Keyspace string
	Table    string
}

func (s *generatorTestSuite) newGenerator(ctx context.Context, target Target, smallTables ...keyspaceTableName) (*generator, *intensityHandler) {
	b := newTableTokenRangeBuilder(target, s.hostDC).Add(s.ranges)
	g := newGenerator(gracefulStopTimeout, newNopProgressManager(), log.NewDevelopment())
	for _, u := range s.units {
		g.Add(ctx, b.Build(u))
	}

	for _, kt := range smallTables {
		g.markSmallTable(kt.Keyspace, kt.Table)
	}

	ctl, ih := s.newController(target.Intensity, target.Parallel, b.MaxParallelRepairs())
	if err := g.Init(ctx, ctl, s.hostPriority); err != nil {
		panic(err)
	}

	return g, ih
}

func (s *generatorTestSuite) newController(intensity float64, parallel, maxParallel int) (controller, *intensityHandler) {
	ih := &intensityHandler{
		logger:      log.NewDevelopment(),
		intensity:   atomic.NewFloat64(intensity),
		parallel:    atomic.NewInt64(int64(parallel)),
		maxParallel: maxParallel,
	}

	var ctl controller
	if s.rowLevelRepair {
		ctl = newRowLevelRepairController(ih, s.hostRangesLimits)
	} else {
		ctl = newDefaultController(ih, s.hostRangesLimits)
	}

	return ctl, ih
}

func (s *generatorTestSuite) Basic(t *testing.T) {
	for _, intensity := range []float64{0, 0.001, 0.25, 0.5, 1, 2, 4} {
		for _, parallel := range []int{0, 1, 2, 3} {
			name := fmt.Sprint("with intensity=", intensity, " parallel=", parallel)
			t.Run(name, func(t *testing.T) {
				t.Parallel()

				ctx := context.Background()
				target := Target{
					DC:        s.dcs,
					Intensity: intensity,
					Parallel:  parallel,
				}
				b := newTableTokenRangeBuilder(target, s.hostDC).Add(s.ranges)
				g := newGenerator(gracefulStopTimeout, newNopProgressManager(), log.NewDevelopment())

				var allRanges []*tableTokenRange
				for _, u := range s.units {
					allRanges = append(allRanges, b.Build(u)...)
					g.Add(ctx, b.Build(u))
				}

				ctl, _ := s.newController(target.Intensity, target.Parallel, b.MaxParallelRepairs())
				if err := g.Init(ctx, ctl, s.hostPriority); err != nil {
					t.Fatal(err)
				}
				go g.Run(ctx)

				w := fakeWorker{
					In:     g.Next(),
					Out:    g.Result(),
					Logger: log.NewDevelopment(),
				}
				jobs := w.drainJobs()

				// Check that all ranges are covered
				var drainedRanges []*tableTokenRange
				for _, j := range jobs {
					drainedRanges = append(drainedRanges, j.Ranges...)
				}
				if diff := cmp.Diff(drainedRanges, allRanges, cmpopts.SortSlices(ttrLess)); diff != "" {
					t.Error("Ranges mismatch diff", diff)
				}
			})
		}
	}
}

func (s *generatorTestSuite) Intensity(t *testing.T) {
	var overMaxOutliers int

	table := []struct {
		Name      string
		Intensity float64
		Assert    func(j job)
	}{
		{
			Name:      "Default",
			Intensity: 1,
			Assert: func(j job) {
				if v := s.hostRangesLimits[j.Host].Default; len(j.Ranges) > v {
					t.Errorf("len(j.Ranges)=%d, expected less than %d", len(j.Ranges), v)
				}
			},
		},
		{
			Name:      "Max",
			Intensity: 0,
			Assert: func(j job) {
				if v := s.hostRangesLimits[j.Host].Max; len(j.Ranges) > v {
					t.Errorf("len(j.Ranges)=%d, expected less than %d", len(j.Ranges), v)
				}
			},
		},
		{
			Name:      "Over max",
			Intensity: 1000,
			Assert: func(j job) {
				if v := s.hostRangesLimits[j.Host].Max; len(j.Ranges) < v {
					overMaxOutliers++
					if overMaxOutliers > 4 {
						t.Errorf("len(j.Ranges)=%d, expected more than %d", len(j.Ranges), v)
					}
				}
			},
		},
	}

	for i := range table {
		test := table[i]
		t.Run(test.Name, func(t *testing.T) {
			ctx := context.Background()

			target := Target{
				DC:        s.dcs,
				Intensity: test.Intensity,
				Parallel:  1,
			}

			g, _ := s.newGenerator(ctx, target)
			go g.Run(ctx)

			w := fakeWorker{
				In:     g.Next(),
				Out:    g.Result(),
				Logger: log.NewDevelopment(),
			}
			jobs := w.drainJobs()

			// Check that ranges follow host limit
			for _, j := range jobs {
				test.Assert(j)
			}
		})
	}
}

func (s *generatorTestSuite) IntensityChange(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	Print("Given: intensity of 1")
	target := Target{
		DC:        s.dcs,
		Intensity: 1,
	}

	Print("Given: running generator")
	g, ih := s.newGenerator(ctx, target)
	generatorFinished := atomic.NewBool(false)
	go func() {
		g.Run(ctx)
		generatorFinished.Store(true)
	}()
	waitGeneratorFillsNext(g)

	Print("Given: worker")
	w := fakeWorker{
		In:       g.Next(),
		Out:      g.Result(),
		DumpJobs: true,
		Logger:   log.NewDevelopment(),
	}

	Print("When: initial jobs are processed")
	jobs := make([]job, len(g.Next()))
	var ok bool
	for i := range jobs {
		jobs[i], ok = w.takeJob()
		if !ok {
			t.Error("Worker couldn't take a job")
		}
	}

	Print("And: intensity is changed to 2")
	if err := ih.SetIntensity(ctx, 2); err != nil {
		t.Fatal(err)
	}
	for _, j := range jobs {
		w.execute(j)
	}

	Print("Then: worker gets 2 token ranges per shard")
	newJob, ok := w.takeJob()
	if !ok {
		t.Error("Worker couldn't take a job")
	}

	const minHostRangesLimitsDefault = 2
	if v := len(newJob.Ranges); v != 2*minHostRangesLimitsDefault {
		t.Errorf("len(newJob.Ranges)=%d, expected %d", len(newJob.Ranges), 2*s.hostRangesLimits[newJob.Host].Default)
	}
}

func (s *generatorTestSuite) Parallel(t *testing.T) {
	table := []struct {
		Name          string
		Parallel      int
		ActiveRepairs int
	}{
		{
			Name:          "Max",
			Parallel:      0,
			ActiveRepairs: 3,
		},
		{
			Name:          "Single",
			Parallel:      1,
			ActiveRepairs: 1,
		},
		{
			Name:          "Multiple",
			Parallel:      3,
			ActiveRepairs: 3,
		},
		{
			Name:          "Multiple over max",
			Parallel:      10,
			ActiveRepairs: 3,
		},
	}

	for i := range table {
		test := table[i]
		t.Run(test.Name, func(t *testing.T) {
			ctx := context.Background()

			target := Target{
				DC:        s.dcs,
				Intensity: 1,
				Parallel:  test.Parallel,
			}

			g, _ := s.newGenerator(ctx, target)
			if s.rowLevelRepair && test.Name == "Max" {
				t.Skip("For row-level-repair max parallelism can span multiple replicas as long as token ranges limits are not exceeded")
			}

			go func() {
				g.Run(ctx)
			}()
			wc := waitGeneratorFillsNext(g)

			workers := make([]fakeWorker, wc)
			for i := range workers {
				workers[i] = fakeWorker{
					In:     g.Next(),
					Out:    g.Result(),
					Logger: log.NewDevelopment(),
				}
			}

			activeRepairs := u64set.New()
			for _, w := range workers {
				job, ok := w.tryTakeJob()
				if ok {
					activeRepairs.Add(job.Ranges[0].ReplicaHash())
				}
			}

			if activeRepairs.Size() != test.ActiveRepairs {
				t.Errorf("activeRepairs.Size()=%d, expected %d", activeRepairs.Size(), test.ActiveRepairs)
			}
		})
	}
}

func (s *generatorTestSuite) SingleDatacenter(t *testing.T) {
	ctx := context.Background()

	target := Target{
		DC: s.dcs[0:1],
	}

	g, _ := s.newGenerator(ctx, target)
	go g.Run(ctx)

	w := fakeWorker{
		In:     g.Next(),
		Out:    g.Result(),
		Logger: log.NewDevelopment(),
	}
	jobs := w.drainJobs()

	if len(jobs) == 0 {
		t.Error("No jobs")
	}
}

func (s *generatorTestSuite) SmallTables(t *testing.T) {
	ctx := context.Background()

	target := Target{
		DC:        s.dcs,
		Intensity: 1,
	}

	// Mark all tables as small
	var smallTables []keyspaceTableName
	for _, u := range s.units {
		for _, t := range u.Tables {
			smallTables = append(smallTables, keyspaceTableName{u.Keyspace, t})
		}
	}
	g, _ := s.newGenerator(ctx, target, smallTables...)
	go g.Run(ctx)

	w := fakeWorker{
		In:     g.Next(),
		Out:    g.Result(),
		Logger: log.NewDevelopment(),
	}
	jobs := w.drainJobs()

	if len(jobs) != tables*replicaSets {
		t.Errorf("len(jobs)=%d, expected %d", len(jobs), replicaSets)
	}
}

func (s *generatorTestSuite) GracefulShutdown(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	target := Target{
		DC: s.dcs,
	}

	generatorFinished := atomic.NewBool(false)
	g, _ := s.newGenerator(ctx, target)
	go func() {
		g.Run(ctx)
		generatorFinished.Store(true)
	}()
	wc := waitGeneratorFillsNext(g)

	Print("Given: multiple workers")
	workers := make([]fakeWorker, wc)
	for i := range workers {
		workers[i] = fakeWorker{
			In:     g.Next(),
			Out:    g.Result(),
			Logger: log.NewDevelopment().With("worker", i),
		}
	}

	Print("When: all workers start repairing")
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
}

func ttrLess(a, b *tableTokenRange) bool {
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

func waitGeneratorFillsNext(g *generator) int {
	l := 0
	for {
		v := len(g.Next())
		if v > 0 && v == l {
			break
		}
		l = v
		time.Sleep(50 * time.Millisecond)
	}

	return l
}
