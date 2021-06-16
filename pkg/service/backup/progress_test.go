// Copyright (C) 2017 ScyllaDB

package backup

import (
	"encoding/json"
	"os"
	"path"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
)

const (
	testSnapshotTag = "sm_20091110230000UTC"
)

func TestAggregateProgress(t *testing.T) {
	t.Parallel()

	var (
		host1 = "host1"
		host2 = "host2"
		time1 = time.Date(2019, 1, 1, 0, 0, 0, 0, time.UTC).Add(-time.Hour)
		time2 = time1.Add(time.Minute)
		time3 = time2.Add(time.Minute)
		time4 = time1.Add(5 * time.Minute)
	)

	run1 := &Run{
		SnapshotTag: testSnapshotTag,
		Units: []Unit{
			{
				Keyspace: "ks",
				Tables:   []string{"table1", "table2"},
			},
		},
		DC:        []string{"dc1", "dc2"},
		StartTime: time1,
	}

	run2 := &Run{
		SnapshotTag: testSnapshotTag,
		Units: []Unit{
			{
				Keyspace: "ks",
				Tables:   []string{"table1", "table2", "table3"},
			},
		},
		DC:        []string{"dc1", "dc2"},
		StartTime: time1,
	}

	runNoUnits := &Run{
		SnapshotTag: testSnapshotTag,
		DC:          []string{"dc3"},
		StartTime:   time1,
	}

	table := []struct {
		Name        string
		Run         *Run
		RunProgress []*RunProgress
		Golden      string
	}{
		{
			Name:        "run with no progress",
			Run:         run1,
			RunProgress: nil,
			Golden:      "no_run_progress.golden.json",
		},
		{
			Name: "run with no units",
			Run:  runNoUnits,
			RunProgress: []*RunProgress{
				{},
			},
			Golden: "no_units.golden.json",
		},
		{
			Name: "run with success progress",
			Run:  run1,
			RunProgress: []*RunProgress{
				{
					Host:        host1,
					Unit:        0,
					TableName:   "table1",
					StartedAt:   &time1,
					CompletedAt: &time3,
					Error:       "",
					Size:        17,
					Skipped:     10,
					Uploaded:    7,
				},
				{
					Host:      host1,
					Unit:      0,
					TableName: "table2",
					StartedAt: &time1,
					Error:     "",
					Size:      10,
					Uploaded:  5,
				},
				{
					Host:        host2,
					Unit:        0,
					TableName:   "table1",
					StartedAt:   &time1,
					CompletedAt: &time4,
					Error:       "",
					Size:        10,
					Uploaded:    10,
				},
				{
					Host:      host2,
					Unit:      0,
					TableName: "table2",
					StartedAt: &time1,
					Error:     "",
					Size:      20,
					Uploaded:  13,
				},
			},
			Golden: "on_success.golden.json",
		},
		{
			Name: "run with success progress on non-started tables",
			Run:  run2,
			RunProgress: []*RunProgress{
				{
					Host:        host1,
					Unit:        0,
					TableName:   "table1",
					StartedAt:   &time1,
					CompletedAt: &time3,
					Error:       "",
					Size:        17,
					Skipped:     10,
					Uploaded:    7,
				},
				{
					Host:      host1,
					Unit:      0,
					TableName: "table2",
					StartedAt: &time1,
					Error:     "",
					Size:      10,
					Uploaded:  5,
				},
				{
					Host:        host2,
					Unit:        0,
					TableName:   "table1",
					StartedAt:   &time1,
					CompletedAt: &time4,
					Error:       "",
					Size:        10,
					Uploaded:    10,
				},
				{
					Host:      host2,
					Unit:      0,
					TableName: "table2",
					StartedAt: &time1,
					Error:     "",
					Size:      20,
					Uploaded:  13,
				},
			},
			Golden: "on_success_not_started.golden.json",
		},
		{
			Name: "run with error progress",
			Run:  run1,
			RunProgress: []*RunProgress{
				{
					Host:        host1,
					Unit:        0,
					TableName:   "table1",
					StartedAt:   &time1,
					CompletedAt: &time2,
					Error:       "",
					Size:        10,
					Uploaded:    10,
				},
				{
					Host:        host1,
					Unit:        0,
					TableName:   "table2",
					StartedAt:   &time1,
					CompletedAt: &time2,
					Error:       "error1, error2",
					Size:        20,
					Uploaded:    10,
					Failed:      10,
				},
				{
					Host:        host2,
					Unit:        0,
					TableName:   "table1",
					StartedAt:   &time1,
					CompletedAt: &time4,
					Error:       "",
					Size:        10,
					Uploaded:    10,
				},
				{
					Host:      host2,
					Unit:      0,
					TableName: "table2",
					StartedAt: &time1,
					Error:     "",
					Size:      10,
					Uploaded:  3,
				},
			},
			Golden: "on_error.golden.json",
		},
	}

	opts := cmp.Options{
		cmp.AllowUnexported(Progress{}, HostProgress{}, KeyspaceProgress{}, TableProgress{}),
		cmpopts.IgnoreUnexported(progress{}),
	}

	for i := range table {
		test := table[i]

		t.Run(test.Name, func(t *testing.T) {
			t.Parallel()

			f, err := os.Open(path.Join("testdata/aggregate_progress", test.Golden))
			if err != nil {
				t.Fatal(err)
			}
			var expected Progress
			if err := json.NewDecoder(f).Decode(&expected); err != nil {
				t.Fatal(err)
			}
			f.Close()
			p, err := aggregateProgress(test.Run, &testVisitor{prog: test.RunProgress})
			if err != nil {
				t.Fatal(err)
			}
			if diff := cmp.Diff(expected, p, opts); diff != "" {
				t.Error(diff)
			}
		})
	}
}

type testVisitor struct {
	prog []*RunProgress
}

func (i *testVisitor) ForEach(visit func(*RunProgress) error) error {
	for _, pr := range i.prog {
		if err := visit(pr); err != nil {
			return err
		}
	}
	return nil
}
