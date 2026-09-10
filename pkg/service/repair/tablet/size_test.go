// Copyright (C) 2026 ScyllaDB

package tablet

import (
	"math"
	"testing"
)

func TestTableSizesWeight(t *testing.T) {
	testCases := []struct {
		name  string
		sizes tableSizes
		ks    string
		tab   string
		want  float64
	}{
		{
			name:  "no tables",
			sizes: tableSizes{size: map[tableKey]int64{}},
			ks:    "ks",
			tab:   "tab",
			want:  0,
		},
		{
			name: "weighted by size",
			sizes: tableSizes{
				size: map[tableKey]int64{
					{keyspace: "ks", table: "big"}:   750,
					{keyspace: "ks", table: "small"}: 250,
				},
				total: 1000,
			},
			ks:   "ks",
			tab:  "big",
			want: 0.75,
		},
		{
			name: "all tables empty are weighted equally",
			sizes: tableSizes{
				size: map[tableKey]int64{
					{keyspace: "ks", table: "a"}: 0,
					{keyspace: "ks", table: "b"}: 0,
					{keyspace: "ks", table: "c"}: 0,
					{keyspace: "ks", table: "d"}: 0,
				},
				total: 0,
			},
			ks:   "ks",
			tab:  "a",
			want: 0.25,
		},
		{
			name: "unknown table has no weight",
			sizes: tableSizes{
				size:  map[tableKey]int64{{keyspace: "ks", table: "a"}: 100},
				total: 100,
			},
			ks:   "ks",
			tab:  "missing",
			want: 0,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			got := tc.sizes.weight(tc.ks, tc.tab)
			if math.Abs(got-tc.want) > 1e-9 {
				t.Fatalf("weight(%q, %q) = %v, expected %v", tc.ks, tc.tab, got, tc.want)
			}
		})
	}
}

func TestTableSizesWeightSumsToOne(t *testing.T) {
	ts := tableSizes{
		size: map[tableKey]int64{
			{keyspace: "ks", table: "a"}: 1,
			{keyspace: "ks", table: "b"}: 2,
			{keyspace: "ks", table: "c"}: 3,
		},
		total: 6,
	}

	var sum float64
	for tk := range ts.size {
		sum += ts.weight(tk.keyspace, tk.table)
	}
	if math.Abs(sum-1) > 1e-9 {
		t.Fatalf("sum of weights = %v, expected 1", sum)
	}
}
