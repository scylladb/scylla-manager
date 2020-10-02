// Copyright (C) 2017 ScyllaDB

package repair

import (
	"context"
	"sort"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	. "github.com/scylladb/mermaid/pkg/testutils"
	"github.com/scylladb/mermaid/pkg/util/timeutc"
)

type nopProgressManager struct {
}

var _ progressManager = &nopProgressManager{}

func newNopProgressManager() progressManager {
	return &nopProgressManager{}
}

func (pm *nopProgressManager) Init(ctx context.Context, ttrs []*tableTokenRange) error {
	return nil
}

func (pm *nopProgressManager) OnJobStart(ctx context.Context, job job) error {
	return nil
}

func (pm *nopProgressManager) OnJobResult(ctx context.Context, job jobResult) error {
	return nil
}

func (pm *nopProgressManager) OnScyllaJobStart(ctx context.Context, job job, jobID int32) error {
	return nil
}

func (pm *nopProgressManager) OnScyllaJobEnd(ctx context.Context, job job, jobID int32) error {
	return nil
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
			var v []visitorTuple
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

type visitorTuple struct {
	Progress  *RunProgress
	Intervals intervalSlice
}

type testVisitor struct {
	args []visitorTuple
}

func (v *testVisitor) ForEach(visit func(*RunProgress, intervalSlice)) error {
	for _, a := range v.args {
		visit(a.Progress, a.Intervals)
	}
	return nil
}

func staticIntensity() (float64, int) {
	return 666, 6
}

func TestIntervalSliceDuration(t *testing.T) {
	t.Parallel()

	t0 := timeutc.Now().Add(-11 * time.Second)
	var ts []*time.Time
	for i := 0; i < 10; i++ {
		tmp := t0.Add(time.Second)
		ts = append(ts, &tmp)
		t0 = tmp
	}

	table := []struct {
		Name      string
		Intervals intervalSlice
		Golden    time.Duration
	}{
		{
			"Empty",
			intervalSlice{},
			0,
		},
		{
			"Non overlapping intervals",
			genIntervals(ts[0], ts[1], ts[1], ts[2], ts[2], ts[3]),
			3 * time.Second,
		},
		{
			"Overlapping intervals",
			genIntervals(ts[0], ts[2], ts[1], ts[3]),
			3 * time.Second,
		},
		{
			"Intervals with gaps",
			genIntervals(ts[0], ts[2], ts[1], ts[3], ts[5], ts[7], ts[5], ts[7]),
			5 * time.Second,
		},
		{
			"Intervals that don't end",
			genIntervals(ts[0], ts[2], ts[1], nil, ts[5], nil),
			9 * time.Second,
		},
	}

	for _, row := range table {
		t.Run(row.Name, func(t *testing.T) {
			got := row.Intervals.Duration(ts[9])
			if a, b := timeDiffMargin(row.Golden); got < a || got > b {
				t.Errorf("Duration() = %s, Expected %s", got, row.Golden)
			}
		})
	}
}

func genIntervals(ts ...*time.Time) intervalSlice {
	var ints intervalSlice
	for i := 0; i < len(ts); i += 2 {
		ints = append(ints, interval{Start: *ts[i], End: ts[i+1]})
	}

	return ints
}

func timeDiffMargin(d time.Duration) (time.Duration, time.Duration) {
	e := time.Duration(float64(d) * 0.001)
	return d - e, d + e
}
