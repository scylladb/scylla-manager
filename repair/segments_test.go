// Copyright (C) 2017 ScyllaDB

package repair

import (
	"testing"

	"github.com/google/go-cmp/cmp"
)

func TestDumpSegments(t *testing.T) {
	table := []struct {
		S []*Segment
		E string
	}{
		{},
		{
			S: []*Segment{{0, 1}},
			E: "0:1",
		},
		{
			S: []*Segment{{0, 1}, {1, 2}, {3, 4}},
			E: "0:1,1:2,3:4",
		},
	}

	for _, test := range table {
		if diff := cmp.Diff(dumpSegments(test.S), test.E); diff != "" {
			t.Fatal(diff)
		}
	}
}

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

func TestSegmentsContainStartToken(t *testing.T) {
	table := []struct {
		S []*Segment
		T int64
		P int
		E bool
	}{
		{},
		{
			S: []*Segment{{0, 10}},
			T: -1,
			P: 0,
			E: false,
		},
		{
			S: []*Segment{{0, 10}, {10, 20}, {30, 40}},
			T: 10,
			P: 1,
			E: true,
		},
	}

	for _, test := range table {
		p, e := segmentsContainStartToken(test.S, test.T)
		if test.P != p || test.E != e {
			t.Fatal(test)
		}
	}
}
