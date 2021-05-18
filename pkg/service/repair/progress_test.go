// Copyright (C) 2017 ScyllaDB

package repair

import (
	"context"
	"sort"
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	. "github.com/scylladb/scylla-manager/pkg/testutils"
)

type nopProgressManager struct{}

var _ progressManager = &nopProgressManager{}

func newNopProgressManager() progressManager {
	return &nopProgressManager{}
}

func (pm *nopProgressManager) Init(ctx context.Context, ttrs []*tableTokenRange) error {
	return nil
}

func (pm *nopProgressManager) OnJobResult(ctx context.Context, job jobResult) {
}

func (pm *nopProgressManager) OnScyllaJobStart(ctx context.Context, job job, jobID int32) {
}

func (pm *nopProgressManager) OnScyllaJobEnd(ctx context.Context, job job, jobID int32) {
}

func (pm *nopProgressManager) CheckRepaired(ttr *tableTokenRange) bool {
	return false
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
			var v []*RunProgress
			ReadInputJSONFile(t, &v)

			res, err := aggregateProgress(staticIntensity, &testVisitor{v})
			if err != nil {
				t.Error(err)
			}
			var golden Progress
			SaveGoldenJSONFileIfNeeded(t, golden)
			LoadGoldenJSONFile(t, &golden)
			if diff := cmp.Diff(golden, res, opts); diff != "" {
				t.Error(name, diff)
			}
		})
	}
}

type testVisitor struct {
	progs []*RunProgress
}

func (v *testVisitor) ForEach(visit func(*RunProgress)) error {
	for _, p := range v.progs {
		visit(p)
	}
	return nil
}

func staticIntensity() (float64, int) {
	return 666, 6
}
