// Copyright (C) 2017 ScyllaDB

package repair

import (
	"testing"

	"github.com/google/go-cmp/cmp"
)

func TestRetryIteratorNext(t *testing.T) {
	var ri repairIterator

	modifiers := []func(){
		func() {},
		func() { ri.OnSuccess() },
		func() { ri.OnError() },
	}

	for _, postFunc := range modifiers {
		ri = &retryIterator{
			segments: segments{{0, 1}, {1, 2}, {3, 4}},
			progress: &RunProgress{
				SegmentErrorStartTokens: []int64{1, 3},
			},
			segmentsPerRepair: 1,
		}

		var actual []int
		for {
			start, end, ok := ri.Next()
			t.Log(start, end, ok)
			if !ok {
				break
			}
			postFunc()

			actual = append(actual, start)
		}

		if diff := cmp.Diff(actual, []int{1, 2}); diff != "" {
			t.Fatal(diff)
		}
	}
}

func TestForwardIteratorNext(t *testing.T) {
	var ri repairIterator

	modifiers := []func(){
		func() {},
		func() { ri.OnSuccess() },
		func() { ri.OnError() },
	}

	for _, postFunc := range modifiers {
		ri = &forwardIterator{
			segments:          segments{{0, 1}, {1, 2}, {3, 4}},
			progress:          &RunProgress{},
			segmentsPerRepair: 1,
		}

		var actual []int
		for {
			start, end, ok := ri.Next()
			t.Log(start, end, ok)
			if !ok {
				break
			}
			postFunc()

			actual = append(actual, start)
		}

		if diff := cmp.Diff(actual, []int{0, 1, 2}); diff != "" {
			t.Fatal(diff)
		}
	}
}
