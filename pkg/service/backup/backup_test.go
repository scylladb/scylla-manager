// Copyright (C) 2017 ScyllaDB

package backup

import (
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/scylladb/scylla-manager/backupspec"
)

func TestFilterDCLocations(t *testing.T) {
	t.Parallel()

	table := []struct {
		Name      string
		Locations []backupspec.Location
		DCs       []string
		Expect    []backupspec.Location
	}{
		{
			Name:      "empty locations",
			Locations: []backupspec.Location{},
			DCs:       []string{"dc1"},
			Expect:    nil,
		},
		{
			Name:      "empty dcs",
			Locations: []backupspec.Location{{DC: "dc1"}},
			DCs:       []string{},
			Expect:    nil,
		},
		{
			Name:      "one location with matching dc",
			Locations: []backupspec.Location{{DC: "dc1"}},
			DCs:       []string{"dc1"},
			Expect:    []backupspec.Location{{DC: "dc1"}},
		},
		{
			Name:      "one location with no matching dcs",
			Locations: []backupspec.Location{{DC: "dc1"}},
			DCs:       []string{"dc2"},
			Expect:    nil,
		},
		{
			Name:      "multiple locations with matching dcs",
			Locations: []backupspec.Location{{DC: "dc1"}, {DC: "dc2"}},
			DCs:       []string{"dc1", "dc2"},
			Expect:    []backupspec.Location{{DC: "dc1"}, {DC: "dc2"}},
		},
		{
			Name:      "multiple locations with matching and non-matching dcs",
			Locations: []backupspec.Location{{DC: "dc1"}, {DC: "dc2"}, {DC: "dc3"}},
			DCs:       []string{"dc1", "dc2"},
			Expect:    []backupspec.Location{{DC: "dc1"}, {DC: "dc2"}},
		},
		{
			Name:      "multiple locations with non-matching dcs",
			Locations: []backupspec.Location{{DC: "dc1"}, {DC: "dc2"}, {DC: "dc3"}},
			DCs:       []string{"dc4", "dc5"},
			Expect:    nil,
		},
	}

	for i := range table {
		test := table[i]

		t.Run(test.Name, func(t *testing.T) {
			t.Parallel()

			if diff := cmp.Diff(test.Expect, FilterDCs(test.Locations, test.DCs)); diff != "" {
				t.Error(diff)
			}
		})
	}
}

func TestFilterDCLimit(t *testing.T) {
	t.Parallel()

	table := []struct {
		Name     string
		DCLimits []DCLimit
		DCs      []string
		Expect   []DCLimit
	}{
		{
			Name:     "empty locations",
			DCLimits: []DCLimit{},
			DCs:      []string{"dc1"},
			Expect:   nil,
		},
		{
			Name:     "empty dcs",
			DCLimits: []DCLimit{{DC: "dc1"}},
			DCs:      []string{},
			Expect:   nil,
		},
		{
			Name:     "one location with matching dc",
			DCLimits: []DCLimit{{DC: "dc1"}},
			DCs:      []string{"dc1"},
			Expect:   []DCLimit{{DC: "dc1"}},
		},
		{
			Name:     "one location with no matching dcs",
			DCLimits: []DCLimit{{DC: "dc1"}},
			DCs:      []string{"dc2"},
			Expect:   nil,
		},
		{
			Name:     "multiple locations with matching dcs",
			DCLimits: []DCLimit{{DC: "dc1"}, {DC: "dc2"}},
			DCs:      []string{"dc1", "dc2"},
			Expect:   []DCLimit{{DC: "dc1"}, {DC: "dc2"}},
		},
		{
			Name:     "multiple locations with matching and non-matching dcs",
			DCLimits: []DCLimit{{DC: "dc1"}, {DC: "dc2"}, {DC: "dc3"}},
			DCs:      []string{"dc1", "dc2"},
			Expect:   []DCLimit{{DC: "dc1"}, {DC: "dc2"}},
		},
		{
			Name:     "multiple locations with non-matching dcs",
			DCLimits: []DCLimit{{DC: "dc1"}, {DC: "dc2"}, {DC: "dc3"}},
			DCs:      []string{"dc4", "dc5"},
			Expect:   nil,
		},
	}

	for i := range table {
		test := table[i]

		t.Run(test.Name, func(t *testing.T) {
			t.Parallel()

			if diff := cmp.Diff(test.Expect, FilterDCs(test.DCLimits, test.DCs)); diff != "" {
				t.Error(diff)
			}
		})
	}
}

//func TestBackupStageName(t *testing.T) {
//	t.Parallel()
//
//	for _, s := range backup.StageOrder() {
//		if s != backup.StageDone && managerclient.BackupStageName(string(s)) == "" {
//			t.Errorf("%s.Name() is empty", s)
//		}
//	}
//}

func TestRenameScyllaManifest(t *testing.T) {
	t.Parallel()

	table := []struct {
		SnapshotTag    string
		NodeID         string
		ScyllaManifest string
		Expected       string
	}{
		{
			SnapshotTag:    "sm_20230101120000UTC",
			NodeID:         "f47ac10b-58cc-4372-a567-0e02b2c3d479",
			ScyllaManifest: backupspec.ScyllaManifest,
			Expected:       "tag_sm_20230101120000UTC_node_f47ac10b-58cc-4372-a567-0e02b2c3d479_" + backupspec.ScyllaManifest,
		},
		{
			SnapshotTag:    "sm_20260101000000UTC",
			NodeID:         "00000000-0000-0000-0000-000000000000",
			ScyllaManifest: backupspec.ScyllaManifest,
			Expected:       "tag_sm_20260101000000UTC_node_00000000-0000-0000-0000-000000000000_" + backupspec.ScyllaManifest,
		},
	}

	for _, tc := range table {
		t.Run(tc.Expected, func(t *testing.T) {
			t.Parallel()
			got := renameScyllaManifest(tc.SnapshotTag, tc.NodeID, tc.ScyllaManifest)
			if got != tc.Expected {
				t.Errorf("renameScyllaManifest(%q, %q, %q) = %q, expected %q",
					tc.SnapshotTag, tc.NodeID, tc.ScyllaManifest, got, tc.Expected)
			}
		})
	}
}
