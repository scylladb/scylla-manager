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
		name        string
		rows        string
		snapshotTag string
		expected    Progress
	}{
		{
			name:        "Only data stage",
			rows:        "testdata/only_data.json",
			snapshotTag: "snapshot_tag",
		},
		{
			name:        "Only alter tombstone_gc mode stage",
			rows:        "testdata/only_tgc.json",
			snapshotTag: "snapshot_tag",
		},
		{
			name:        "Only drop views stage",
			rows:        "testdata/only_drop_views.json",
			snapshotTag: "snapshot_tag",
		},
		{
			name:        "Data and drop views stage",
			rows:        "testdata/data_and_drop_views.json",
			snapshotTag: "snapshot_tag",
		},
		{
			name:        "Data and recreate views stage",
			rows:        "testdata/data_and_recreate_views.json",
			snapshotTag: "snapshot_tag",
		},
		{
			name:        "All stages",
			rows:        "testdata/all_stages.json",
			snapshotTag: "snapshot_tag",
		},
		{
			name:        "Data stage in progress",
			rows:        "testdata/data_stage_in_progress.json",
			snapshotTag: "snapshot_tag",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			iter, stop := newDBTest(t, tc.rows)
			defer stop()

			pr := (&worker{}).aggregateProgress(iter, tc.snapshotTag)

			testutils.SaveGoldenJSONFileIfNeeded(t, &pr)
			var expectedProgress Progress
			testutils.LoadGoldenJSONFile(t, &expectedProgress)

			sortKeyspacesOpt := cmpopts.SortSlices(func(a, b KeyspaceProgress) bool {
				return a.Keyspace < b.Keyspace
			})
			sortTablesOpt := cmpopts.SortSlices(func(a, b TableProgress) bool {
				return a.Table < b.Table
			})
			sortHostsOpt := cmpopts.SortSlices(func(a, b HostProgress) bool {
				return a.Host < b.Host
			})
			unexportedOpts := cmp.AllowUnexported(Progress{}, KeyspaceProgress{}, TableProgress{})
			opts := []cmp.Option{
				sortKeyspacesOpt,
				sortTablesOpt,
				sortHostsOpt,
				unexportedOpts,
			}
			if diff := cmp.Diff(expectedProgress, pr, opts...); diff != "" {
				t.Fatalf("Actual != Expected:\n%v", diff)
			}
		})
	}
}

func newDBTest(t *testing.T, dataPath string) (it *dbIterTest, stop func()) {
	t.Helper()
	data, err := os.ReadFile(dataPath)
	if err != nil {
		t.Fatalf("open test data file: %v", err)
	}
	var rows []RunProgress
	if err := json.Unmarshal(data, &rows); err != nil {
		t.Fatalf("unmarshal test data file: %v", err)
	}
	next, stop := iter.Pull(slices.Values(rows))
	return &dbIterTest{
		next: next,
	}, stop
}

type dbIterTest struct {
	next func() (RunProgress, bool)
}

func (db *dbIterTest) StructScan(v any) bool {
	pr, ok := v.(*RunProgress)
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
