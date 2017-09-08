package repair

import (
	"testing"

	"github.com/google/go-cmp/cmp"
)

func TestMergeSegments(t *testing.T) {
	table := []struct {
		S []*Segment
		E []*Segment
	}{
		{},
		{
			S: []*Segment{{0, 1}},
			E: []*Segment{{0, 1}},
		},
		{
			S: []*Segment{{0, 1}, {1, 2}, {3, 4}},
			E: []*Segment{{0, 2}, {3, 4}},
		},
		{
			S: []*Segment{{0, 1}, {1, 2}, {1, 4}},
			E: []*Segment{{0, 4}},
		},
		{
			S: []*Segment{{0, 5}, {1, 2}, {1, 4}},
			E: []*Segment{{0, 5}},
		},
	}

	for _, test := range table {
		if diff := cmp.Diff(mergeSegments(test.S), test.E); diff != "" {
			t.Fatal(diff)
		}
	}
}

func TestSplitSegments(t *testing.T) {
	table := []struct {
		S []*Segment
		L int64
		E []*Segment
	}{
		{},
		{
			S: []*Segment{{0, 10}},
			L: -1,
			E: []*Segment{{0, 10}},
		},
		{
			S: []*Segment{{0, 10}, {10, 20}, {30, 40}},
			L: 10,
			E: []*Segment{{0, 10}, {10, 20}, {30, 40}},
		},
		{
			S: []*Segment{{0, 10}, {10, 20}, {30, 40}},
			L: 6,
			E: []*Segment{{0, 6}, {6, 10}, {10, 16}, {16, 20}, {30, 36}, {36, 40}},
		},
	}

	for _, test := range table {
		s := splitSegments(test.S, test.L)
		if diff := cmp.Diff(s, test.E); diff != "" {
			t.Fatal(diff)
		}
		if len(s) != cap(s) {
			t.Fatal("wrong size calculation")
		}
	}
}
