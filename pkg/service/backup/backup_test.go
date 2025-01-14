// Copyright (C) 2017 ScyllaDB

package backup

import (
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/scylladb/scylla-manager/v3/pkg/util/backupmanifest"
)

func TestFilterDCLocations(t *testing.T) {
	t.Parallel()

	table := []struct {
		Name      string
		Locations []backupmanifest.Location
		DCs       []string
		Expect    []backupmanifest.Location
	}{
		{
			Name:      "empty locations",
			Locations: []backupmanifest.Location{},
			DCs:       []string{"dc1"},
			Expect:    nil,
		},
		{
			Name:      "empty dcs",
			Locations: []backupmanifest.Location{{DC: "dc1"}},
			DCs:       []string{},
			Expect:    nil,
		},
		{
			Name:      "one location with matching dc",
			Locations: []backupmanifest.Location{{DC: "dc1"}},
			DCs:       []string{"dc1"},
			Expect:    []backupmanifest.Location{{DC: "dc1"}},
		},
		{
			Name:      "one location with no matching dcs",
			Locations: []backupmanifest.Location{{DC: "dc1"}},
			DCs:       []string{"dc2"},
			Expect:    nil,
		},
		{
			Name:      "multiple locations with matching dcs",
			Locations: []backupmanifest.Location{{DC: "dc1"}, {DC: "dc2"}},
			DCs:       []string{"dc1", "dc2"},
			Expect:    []backupmanifest.Location{{DC: "dc1"}, {DC: "dc2"}},
		},
		{
			Name:      "multiple locations with matching and non-matching dcs",
			Locations: []backupmanifest.Location{{DC: "dc1"}, {DC: "dc2"}, {DC: "dc3"}},
			DCs:       []string{"dc1", "dc2"},
			Expect:    []backupmanifest.Location{{DC: "dc1"}, {DC: "dc2"}},
		},
		{
			Name:      "multiple locations with non-matching dcs",
			Locations: []backupmanifest.Location{{DC: "dc1"}, {DC: "dc2"}, {DC: "dc3"}},
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
