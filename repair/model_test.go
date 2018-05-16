// Copyright (C) 2017 ScyllaDB

package repair

import (
	"testing"
)

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
