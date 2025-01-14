// Copyright (C) 2017 ScyllaDB

package backup

import (
	"encoding/json"
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/scylladb/scylla-manager/v3/pkg/util/backupmanifest"
)

func TestDCLimitMarshalUnmarshalText(t *testing.T) {
	t.Parallel()

	table := []struct {
		Name    string
		DCLimit DCLimit
	}{
		{
			Name: "with dc",
			DCLimit: DCLimit{
				DC:    "dc",
				Limit: 100,
			},
		},
		{
			Name: "without dc",
			DCLimit: DCLimit{
				Limit: 100,
			},
		},
	}

	for i := range table {
		test := table[i]

		t.Run(test.Name, func(t *testing.T) {
			t.Parallel()

			golden := test.DCLimit
			b, err := golden.MarshalText()
			if err != nil {
				t.Error(golden, err)
			}
			var r DCLimit
			if err := r.UnmarshalText(b); err != nil {
				t.Error(err)
			}
			if golden != r {
				t.Errorf("Got %s, expected %s", r, golden)
			}
		})
	}
}

func TestExtractLocations(t *testing.T) {
	t.Parallel()

	table := []struct {
		Name     string
		Json     string
		Location []backupmanifest.Location
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
			Location: []backupmanifest.Location{
				{DC: "dc", Provider: backupmanifest.S3, Path: "foo"},
				{Provider: backupmanifest.S3, Path: "bar"},
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
