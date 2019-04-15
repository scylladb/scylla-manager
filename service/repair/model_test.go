// Copyright (C) 2017 ScyllaDB

package repair

import (
	"testing"

	"github.com/google/go-cmp/cmp"
)

func TestTokenRangesKindMarshalUnmarshalText(t *testing.T) {
	for _, k := range []TokenRangesKind{DCPrimaryTokenRanges, PrimaryTokenRanges, NonPrimaryTokenRanges, AllTokenRanges} {
		b, err := k.MarshalText()
		if err != nil {
			t.Error(k, err)
		}
		var l TokenRangesKind
		if err := l.UnmarshalText(b); err != nil {
			t.Error(err)
		}
		if k != l {
			t.Errorf("got %s, expected %s", l, k)
		}
	}
}

func TestProgressAddProgress(t *testing.T) {
	p := progress{
		segmentCount:   100,
		segmentSuccess: 10,
		segmentError:   1,
	}
	p.addProgress(p)

	e := progress{
		segmentCount:   200,
		segmentSuccess: 20,
		segmentError:   2,
	}
	if diff := cmp.Diff(p, e, cmp.AllowUnexported(progress{})); diff != "" {
		t.Fatal(diff)
	}
}

func TestProgressCalculateProgress(t *testing.T) {
	tests := []struct {
		P progress
		C float64
		F float64
	}{
		{
			P: progress{},
			C: 0,
			F: 0,
		},
		{
			P: progress{segmentSuccess: 1, segmentCount: 100},
			C: 1,
			F: 0,
		},
		{
			P: progress{segmentError: 1, segmentCount: 100},
			C: 0,
			F: 1,
		},
		{
			P: progress{segmentSuccess: 90, segmentError: 10, segmentCount: 100},
			C: 90,
			F: 10,
		},
	}

	for i, test := range tests {
		p := test.P.calculateProgress()
		if p.PercentComplete != test.C {
			t.Error(i, "got", p.PercentComplete, "want", test.C)
		}
		if p.PercentFailed != test.F {
			t.Error(i, "got", p.PercentFailed, "want", test.F)
		}
	}
}

func TestRunProgressPercentComplete(t *testing.T) {
	table := []struct {
		P RunProgress
		E int
	}{
		{
			P: RunProgress{},
			E: 0,
		},
		{
			P: RunProgress{SegmentSuccess: 100, SegmentCount: 200},
			E: 50,
		},
		{
			P: RunProgress{SegmentSuccess: 100, SegmentCount: 200, SegmentError: 100},
			E: 50,
		},
		{
			P: RunProgress{SegmentSuccess: 1, SegmentCount: 100},
			E: 1,
		},
	}

	for i, test := range table {
		if test.P.PercentComplete() != test.E {
			t.Error(i, "expected", test.E, "got", test.P.PercentComplete())
		}
	}
}
