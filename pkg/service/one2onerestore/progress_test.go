// Copyright (C) 2025 ScyllaDB

package one2onerestore

import (
	"encoding/json"
	"iter"
	"os"
	"slices"
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	"github.com/scylladb/scylla-manager/v3/pkg/testutils"
)

func TestAggregateProgress(t *testing.T) {
	testCases := []struct {
		name      string
		tableRows string
		viewRows  string
		expected  Progress
	}{
		{
			name:      "Only tables progress",
			tableRows: "testdata/1.run_table_progress.json",
			viewRows:  "testdata/1.run_view_progress.json",
		},
		{
			name:      "Only views progress",
			tableRows: "testdata/2.run_table_progress.json",
			viewRows:  "testdata/2.run_view_progress.json",
		},
		{
			name:      "Tables and views progress, status not started",
			tableRows: "testdata/3.run_table_progress.json",
			viewRows:  "testdata/3.run_view_progress.json",
		},
		{
			name:      "Tables and views progress, status in progress",
			tableRows: "testdata/4.run_table_progress.json",
			viewRows:  "testdata/4.run_view_progress.json",
		},
		{
			name:      "Tables and views progress, status done",
			tableRows: "testdata/5.run_table_progress.json",
			viewRows:  "testdata/5.run_view_progress.json",
		},
		{
			name:      "Tables and views progress, status mixed",
			tableRows: "testdata/6.run_table_progress.json",
			viewRows:  "testdata/6.run_view_progress.json",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			tableIter, stop := newDBTest[RunTableProgress](t, tc.tableRows)
			defer stop()
			viewIter, viewStop := newDBTest[RunViewProgress](t, tc.viewRows)
			defer viewStop()

			pr := (&worker{}).aggregateProgress(tableIter, viewIter)

			testutils.SaveGoldenJSONFileIfNeeded(t, &pr)
			var expectedProgress Progress
			testutils.LoadGoldenJSONFile(t, &expectedProgress)

			unexportedOpts := cmp.AllowUnexported(TableProgress{}, ViewProgress{})
			sortTables := cmpopts.SortSlices(func(a, b TableProgress) bool {
				return a.Keyspace <= b.Keyspace && a.Table <= b.Table
			})
			sortViews := cmpopts.SortSlices(func(a, b ViewProgress) bool {
				return a.Keyspace <= b.Keyspace && a.Table <= b.Table
			})
			opts := []cmp.Option{
				sortTables,
				sortViews,
				unexportedOpts,
			}
			if diff := cmp.Diff(expectedProgress, pr, opts...); diff != "" {
				t.Fatalf("Actual != Expected:\n%v", diff)
			}
		})
	}
}

type runProgress interface {
	RunTableProgress | RunViewProgress
}

func newDBTest[T runProgress](t *testing.T, dataPath string) (it *dbIterTest[T], stop func()) {
	t.Helper()
	data, err := os.ReadFile(dataPath)
	if err != nil {
		t.Fatalf("open test data file: %v", err)
	}
	var rows []T
	if err := json.Unmarshal(data, &rows); err != nil {
		t.Fatalf("unmarshal test data file: %s: %v", dataPath, err)
	}
	next, stop := iter.Pull(slices.Values(rows))
	return &dbIterTest[T]{
		next: next,
	}, stop
}

type dbIterTest[T runProgress] struct {
	next func() (T, bool)
}

func (db *dbIterTest[T]) StructScan(v any) bool {
	pr, ok := v.(*T)
	if !ok {
		return false
	}
	row, ok := db.next()
	if !ok {
		return false
	}
	*pr = row
	return true
}

func toPtr[T any](t T) *T {
	return &t
}
