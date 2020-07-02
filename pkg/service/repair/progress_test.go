// Copyright (C) 2017 ScyllaDB

package repair

import (
	"context"
	"sort"
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	. "github.com/scylladb/mermaid/pkg/testutils"
)

type nopProgressManager struct {
}

var _ progressManager = &nopProgressManager{}

func newNopProgressManager() progressManager {
	return &nopProgressManager{}
}

func (pu *nopProgressManager) Init(ctx context.Context, ttrs []*tableTokenRange) error {
	return nil
}

func (pu *nopProgressManager) Update(ctx context.Context, job jobResult) error {
	return nil
}

func (pu *nopProgressManager) OnStartJob(ctx context.Context, job job) error {
	return nil
}

func (pu *nopProgressManager) CheckRepaired(ttr *tableTokenRange) bool {
	return false
}

func newUpsertProgressFunc(updates *[]RunProgress) func(ctx context.Context, p *RunProgress) error {
	return func(ctx context.Context, p *RunProgress) error {
		if updates != nil {
			*updates = append(*updates, *p)
		}
		return nil
	}
}

func newUpsertStateFunc(updates *[]RunState) func(ctx context.Context, p *RunState) error {
	return func(ctx context.Context, p *RunState) error {
		if updates != nil {
			*updates = append(*updates, *p)
		}
		return nil
	}
}

func newGetStateFunc(states []*RunState) func(ctx context.Context, run *Run) ([]*RunState, error) {
	return func(ctx context.Context, run *Run) ([]*RunState, error) {
		return states, nil
	}
}

// Sorting transformer to compare RunProgress slices that can change order.
var progTrans = cmp.Transformer("Sort", func(in []RunProgress) []RunProgress {
	out := append([]RunProgress(nil), in...) // Copy input to avoid mutating it
	sort.Slice(out, func(i, j int) bool {
		return out[i].Host+out[i].Keyspace+out[i].Table < out[j].Host+out[j].Keyspace+out[j].Table
	})
	return out
})

func TestAggregateProgress(t *testing.T) {
	t.Parallel()

	// Test names
	testNames := []string{
		"empty progress list",
		"multiple progress multi host",
		"single progress single host",
	}

	opts := cmp.Options{
		cmp.AllowUnexported(Progress{}, Unit{}, HostProgress{}, TableProgress{}),
		cmpopts.IgnoreUnexported(progress{}),
	}

	for _, name := range testNames {
		t.Run(name, func(t *testing.T) {
			var prog []*RunProgress
			ReadInputJSONFile(t, &prog)

			res, err := aggregateProgress(staticIntensity, &testVisitor{prog})
			if err != nil {
				t.Error(err)
			}
			var golden Progress
			SaveGoldenJSONFileIfNeeded(t, golden)
			LoadGoldenJSONFile(t, &golden)
			if diff := cmp.Diff(res, golden, opts); diff != "" {
				t.Error(name, diff)
			}
		})
	}
}

type testVisitor struct {
	prog []*RunProgress
}

func (i *testVisitor) ForEach(visit func(*RunProgress)) error {
	for _, pr := range i.prog {
		visit(pr)
	}
	return nil
}

func staticIntensity(host string) float64 {
	return 666
}
