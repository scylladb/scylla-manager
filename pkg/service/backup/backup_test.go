// Copyright (C) 2017 ScyllaDB

package backup

import (
	"encoding/json"
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/scylladb/scylla-manager/pkg/backup"
)

func TestFilterDCLocations(t *testing.T) {
	t.Parallel()

	table := []struct {
		Name      string
		Locations []backup.Location
		DCs       []string
		Expect    []backup.Location
	}{
		{
			Name:      "empty locations",
			Locations: []backup.Location{},
			DCs:       []string{"dc1"},
			Expect:    nil,
		},
		{
			Name:      "empty dcs",
			Locations: []backup.Location{{DC: "dc1"}},
			DCs:       []string{},
			Expect:    nil,
		},
		{
			Name:      "one location with matching dc",
			Locations: []backup.Location{{DC: "dc1"}},
			DCs:       []string{"dc1"},
			Expect:    []backup.Location{{DC: "dc1"}},
		},
		{
			Name:      "one location with no matching dcs",
			Locations: []backup.Location{{DC: "dc1"}},
			DCs:       []string{"dc2"},
			Expect:    nil,
		},
		{
			Name:      "multiple locations with matching dcs",
			Locations: []backup.Location{{DC: "dc1"}, {DC: "dc2"}},
			DCs:       []string{"dc1", "dc2"},
			Expect:    []backup.Location{{DC: "dc1"}, {DC: "dc2"}},
		},
		{
			Name:      "multiple locations with matching and non-matching dcs",
			Locations: []backup.Location{{DC: "dc1"}, {DC: "dc2"}, {DC: "dc3"}},
			DCs:       []string{"dc1", "dc2"},
			Expect:    []backup.Location{{DC: "dc1"}, {DC: "dc2"}},
		},
		{
			Name:      "multiple locations with non-matching dcs",
			Locations: []backup.Location{{DC: "dc1"}, {DC: "dc2"}, {DC: "dc3"}},
			DCs:       []string{"dc4", "dc5"},
			Expect:    nil,
		},
	}

	for i := range table {
		test := table[i]

		t.Run(test.Name, func(t *testing.T) {
			t.Parallel()

			if diff := cmp.Diff(test.Expect, filterDCLocations(test.Locations, test.DCs)); diff != "" {
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

			if diff := cmp.Diff(test.Expect, filterDCLimits(test.DCLimits, test.DCs)); diff != "" {
				t.Error(diff)
			}
		})
	}
}

func TestExtractLocations(t *testing.T) {
	t.Parallel()

	table := []struct {
		Name     string
		Json     string
		Location []backup.Location
	}{
		{
			Name: "Empty",
			Json: "{}",
		},
		{
			Name: "Invalid properties",
			Json: "",
		},
		{
			Name: "Duplicates",
			Json: `{"location": ["dc:s3:foo", "s3:foo", "s3:bar"]}`,
			Location: []backup.Location{
				{DC: "dc", Provider: backup.S3, Path: "foo"},
				{Provider: backup.S3, Path: "bar"},
			},
		},
	}

	for i := range table {
		test := table[i]

		t.Run(test.Name, func(t *testing.T) {
			t.Parallel()

			l, err := extractLocations([]json.RawMessage{[]byte(test.Json)})
			if err != nil {
				t.Log("extractLocations() error", err)
			}
			if diff := cmp.Diff(l, test.Location); diff != "" {
				t.Errorf("extractLocations() = %s, expected %s", l, test.Location)
			}
		})
	}
}
