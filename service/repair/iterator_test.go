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
				SegmentErrorPos: []int{1, 2},
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

		if diff := cmp.Diff(actual, []int{1, 2}); diff != "" {
			t.Fatal(diff)
		}
	}
}

type nextIteration struct {
	Start int
	End   int
	Ok    bool
}

func TestIteratorSegmentsPerRepairChange(t *testing.T) {
	table := []struct {
		Name              string
		Segments          segments
		SuccessEvents     []int
		SPRForward        int
		SPRRetry          int
		ExpectedIteration []nextIteration
	}{
		{
			Name:       "spr eq 1",
			Segments:   segments{{0, 10}, {11, 20}, {21, 30}},
			SPRForward: 1,
			SPRRetry:   1,
			ExpectedIteration: []nextIteration{
				{Start: 0, End: 1, Ok: true}, {Start: 1, End: 2, Ok: true}, {Start: 2, End: 3, Ok: true}, // Forward iterator over each segment
				{Start: 0, End: 1, Ok: true}, {Start: 1, End: 2, Ok: true}, {Start: 2, End: 3, Ok: true}, // Retry iterator over each failed
			},
		},
		{
			Name:       "spr eq 2",
			Segments:   segments{{0, 10}, {11, 20}, {21, 30}},
			SPRForward: 2,
			SPRRetry:   2,
			ExpectedIteration: []nextIteration{
				{Start: 0, End: 2, Ok: true}, {Start: 2, End: 3, Ok: true}, // Forward iterator over two segments
				{Start: 0, End: 2, Ok: true}, {Start: 2, End: 3, Ok: true}, // Retry iterator over each failed
			},
		},
		{
			Name:       "Change spr low to high",
			Segments:   segments{{0, 10}, {11, 20}, {21, 30}},
			SPRForward: 1,
			SPRRetry:   3,
			ExpectedIteration: []nextIteration{
				{Start: 0, End: 1, Ok: true}, {Start: 1, End: 2, Ok: true}, {Start: 2, End: 3, Ok: true}, // Forward iterator over each segment
				{Start: 0, End: 3, Ok: true}, // Retry iterator over all segments
			},
		},
		{
			Name:       "Change spr low to high over total",
			Segments:   segments{{0, 10}, {11, 20}, {21, 30}},
			SPRForward: 1,
			SPRRetry:   10,
			ExpectedIteration: []nextIteration{
				{Start: 0, End: 1, Ok: true}, {Start: 1, End: 2, Ok: true}, {Start: 2, End: 3, Ok: true}, // Forward iterator over each segment
				{Start: 0, End: 3, Ok: true}, // Retry iterator over all segments
			},
		},
		{
			Name:       "Change spr high to low",
			Segments:   segments{{0, 10}, {11, 20}, {21, 30}},
			SPRForward: 3,
			SPRRetry:   1,
			ExpectedIteration: []nextIteration{
				{Start: 0, End: 3, Ok: true},                                                             // Forward iterator over all segments
				{Start: 0, End: 1, Ok: true}, {Start: 1, End: 2, Ok: true}, {Start: 2, End: 3, Ok: true}, // Retry iterator over each segment
			},
		},
		{
			Name:          "Success segments at start",
			Segments:      segments{{0, 10}, {11, 20}, {21, 30}},
			SuccessEvents: []int{0, 1}, // Success on first two segments
			SPRForward:    1,
			SPRRetry:      1,
			ExpectedIteration: []nextIteration{
				{Start: 0, End: 1, Ok: true}, {Start: 1, End: 2, Ok: true}, {Start: 2, End: 3, Ok: true}, // Forward iterator over each segment
				{Start: 2, End: 3, Ok: true}, // Retry iterator over last segment
			},
		},
		{
			Name:          "Success segment in the middle",
			Segments:      segments{{0, 10}, {11, 20}, {21, 30}},
			SuccessEvents: []int{1}, // Success on second segment
			SPRForward:    1,
			SPRRetry:      1,
			ExpectedIteration: []nextIteration{
				{Start: 0, End: 1, Ok: true}, {Start: 1, End: 2, Ok: true}, {Start: 2, End: 3, Ok: true}, // Forward iterator over each segment
				{Start: 0, End: 1, Ok: true}, {Start: 2, End: 3, Ok: true}, // Retry iterator over each segment
			},
		},
		{
			Name:          "Success segment in the middle high spr",
			Segments:      segments{{0, 10}, {11, 20}, {21, 30}},
			SuccessEvents: []int{1}, // Success on second segment
			SPRForward:    1,
			SPRRetry:      10,
			ExpectedIteration: []nextIteration{
				{Start: 0, End: 1, Ok: true}, {Start: 1, End: 2, Ok: true}, {Start: 2, End: 3, Ok: true}, // Forward iterator over each segment
				{Start: 0, End: 1, Ok: true}, {Start: 2, End: 3, Ok: true}, // Retry iterator over each segment
			},
		},
		{
			Name:          "Success segment at the end",
			Segments:      segments{{0, 10}, {11, 20}, {21, 30}},
			SuccessEvents: []int{2}, // Success on last segment
			SPRForward:    1,
			SPRRetry:      1,
			ExpectedIteration: []nextIteration{
				{Start: 0, End: 1, Ok: true}, {Start: 1, End: 2, Ok: true}, {Start: 2, End: 3, Ok: true}, // Forward iterator over each segment
				{Start: 0, End: 1, Ok: true}, {Start: 1, End: 2, Ok: true}, // Retry iterator over each segment
			},
		},
	}

	for _, test := range table {
		t.Run(test.Name, func(t *testing.T) {
			progress := &RunProgress{
				SegmentCount: len(test.Segments),
			}
			fi := &forwardIterator{
				segments:          test.Segments,
				progress:          progress,
				segmentsPerRepair: test.SPRForward,
			}
			ri := &retryIterator{
				segments:          test.Segments,
				progress:          progress,
				segmentsPerRepair: test.SPRRetry,
			}

			var got []nextIteration
			for {
				start, end, ok := fi.Next()
				if !ok {
					break
				}
				success := false
				if test.SuccessEvents != nil {
					for _, ev := range test.SuccessEvents {
						if ev >= start && ev < end {
							success = true
							break
						}
					}
				}
				if success {
					fi.OnSuccess()
				} else {
					fi.OnError()
				}
				got = append(got, nextIteration{start, end, ok})
			}
			for {
				start, end, ok := ri.Next()
				if !ok {
					break
				}
				ri.OnSuccess()
				got = append(got, nextIteration{start, end, ok})
			}

			if diff := cmp.Diff(test.ExpectedIteration, got); diff != "" {
				t.Error(diff)
			}

			if ri.progress.SegmentCount != ri.progress.SegmentSuccess {
				t.Errorf("Expected %d success, got %d success and %d error",
					ri.progress.SegmentCount, ri.progress.SegmentSuccess, ri.progress.SegmentError)
			}
		})
	}
}
