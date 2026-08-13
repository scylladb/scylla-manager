// Copyright (C) 2017 ScyllaDB

package backup

import (
	"encoding/json"
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/scylladb/scylla-manager/backupspec"
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

func TestTaskPropertiesValidate(t *testing.T) {
	t.Parallel()

	gcs := backupspec.Location{Provider: backupspec.GCS, Path: "bucket"}
	s3 := backupspec.Location{Provider: backupspec.S3, Path: "bucket"}

	table := []struct {
		Name   string
		Mutate func(p *taskProperties)
		Error  bool
	}{
		{
			Name:   "unknown retention lock mode",
			Mutate: func(p *taskProperties) { p.RetentionLockMode = "unknown" },
			Error:  true,
		},
		{
			Name:   "disabled retention lock with override",
			Mutate: func(p *taskProperties) { p.OverrideRetentionLock = true },
			Error:  true,
		},
		{
			Name: "unlocked retention lock without retention days",
			Mutate: func(p *taskProperties) {
				p.RetentionLockMode = RetentionLockUnlocked
			},
			Error: true,
		},
		{
			Name: "locked retention lock with count-based retention",
			Mutate: func(p *taskProperties) {
				p.RetentionLockMode = RetentionLockLocked
				p.RetentionDays = new(7)
				p.Retention = new(3)
			},
			Error: true,
		},
		{
			Name: "locked retention lock on non-GCS location",
			Mutate: func(p *taskProperties) {
				p.RetentionLockMode = RetentionLockLocked
				p.RetentionDays = new(7)
				p.Location = []backupspec.Location{s3}
			},
			Error: true,
		},
		{
			Name: "locked retention lock with retention days",
			Mutate: func(p *taskProperties) {
				p.RetentionLockMode = RetentionLockLocked
				p.RetentionDays = new(7)
			},
		},
		{
			Name: "unlocked retention lock with override",
			Mutate: func(p *taskProperties) {
				p.RetentionLockMode = RetentionLockUnlocked
				p.RetentionDays = new(7)
				p.OverrideRetentionLock = true
			},
		},
		{
			Name: "event based hold without retention days",
			Mutate: func(p *taskProperties) {
				p.RetentionLockMode = RetentionLockEventBasedHold
			},
		},
		{
			Name: "event based hold with retention days",
			Mutate: func(p *taskProperties) {
				p.RetentionLockMode = RetentionLockEventBasedHold
				p.RetentionDays = new(7)
			},
		},
		{
			Name: "event based hold with count-based retention",
			Mutate: func(p *taskProperties) {
				p.RetentionLockMode = RetentionLockEventBasedHold
				p.Retention = new(3)
			},
		},
		{
			Name: "event based hold with override",
			Mutate: func(p *taskProperties) {
				p.RetentionLockMode = RetentionLockEventBasedHold
				p.OverrideRetentionLock = true
			},
			Error: true,
		},
		{
			Name: "event based hold on non-GCS location",
			Mutate: func(p *taskProperties) {
				p.RetentionLockMode = RetentionLockEventBasedHold
				p.Location = []backupspec.Location{s3}
			},
			Error: true,
		},
	}

	dcs := []string{"dc1"}
	dcMap := map[string][]string{"dc1": {"h1"}}

	for i := range table {
		test := table[i]

		t.Run(test.Name, func(t *testing.T) {
			t.Parallel()

			p := defaultTaskProperties()
			p.Location = []backupspec.Location{gcs}
			test.Mutate(&p)

			err := p.validate(dcs, dcMap)
			if test.Error && err == nil {
				t.Fatal("validate() expected error, got nil")
			}
			if !test.Error && err != nil {
				t.Fatalf("validate() unexpected error: %s", err)
			}
		})
	}
}

func TestExtractLocations(t *testing.T) {
	t.Parallel()

	table := []struct {
		Name     string
		Json     string
		Location []backupspec.Location
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
			Location: []backupspec.Location{
				{DC: "dc", Provider: backupspec.S3, Path: "foo"},
				{Provider: backupspec.S3, Path: "bar"},
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
