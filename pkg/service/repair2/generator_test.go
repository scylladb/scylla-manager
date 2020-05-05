// Copyright (C) 2017 ScyllaDB

package repair

import (
	"context"
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	"github.com/scylladb/go-log"
	"github.com/scylladb/mermaid/pkg/scyllaclient"
)

type fakeWorker struct {
	In     <-chan job
	Out    chan<- jobResult
	Logger log.Logger
}

func (w fakeWorker) drainJobs(ctx context.Context) (jobs []job) {
	for {
		job, ok := <-w.In
		if !ok {
			w.Logger.Info(ctx, "Done")
			return
		}
		w.Logger.Info(ctx, "Rcv", "job", job)
		jobs = append(jobs, job)
		w.Out <- jobResult{
			job: job,
		}
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
		g := newGenerator(target, log.NewDevelopment())

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
}
