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
	"github.com/scylladb/mermaid/internal/timeutc"
)

func TestProviderMarshalUnmarshalText(t *testing.T) {
	for _, k := range []Provider{S3} {
		b, err := k.MarshalText()
		if err != nil {
			t.Error(k, err)
		}
		var p Provider
		if err := p.UnmarshalText(b); err != nil {
			t.Error(err)
		}
		if k != p {
			t.Errorf("got %s, expected %s", p, k)
		}
	}
}

func TestLocationMarshalUnmarshalText(t *testing.T) {
	table := []struct {
		Name     string
		Location Location
	}{
		{
			Name: "with dc",
			Location: Location{
				DC:       "dc",
				Provider: S3,
				Path:     "my-bucket.domain",
			},
		},
		{
			Name: "without dc",
			Location: Location{
				Provider: S3,
				Path:     "my-bucket.domain",
			},
		},
	}

	for _, test := range table {
		t.Run(test.Name, func(t *testing.T) {
			golden := test.Location
			b, err := golden.MarshalText()
			if err != nil {
				t.Error(golden, err)
			}
			var l Location
			if err := l.UnmarshalText(b); err != nil {
				t.Error(err)
			}
			if golden != l {
				t.Errorf("got %s, expected %s", l, golden)
			}
		})
	}
}

func TestInvalidLocationUnmarshalText(t *testing.T) {
	table := []struct {
		Name     string
		Location string
	}{
		{
			Name:     "empty",
			Location: "",
		},
		{
			Name:     "empty path",
			Location: "s3:",
		},
		{
			Name:     "empty path with dc",
			Location: "dc:s3:",
		},
		{
			Name:     "invalid dc",
			Location: "dc aaa:foo:bar",
		},
		{
			Name:     "invalid provider",
			Location: "foo:bar",
		},
		{
			Name:     "invalid path",
			Location: "s3:name boo",
		},
	}

	for _, test := range table {
		t.Run(test.Name, func(t *testing.T) {
			l := Location{}
			if err := l.UnmarshalText([]byte(test.Location)); err == nil {
				t.Error("expected error")
			}
		})
	}
}

func TestLocationRemotePath(t *testing.T) {
	l := Location{
		Provider: S3,
		Path:     "foo",
	}

	table := []struct {
		Path       string
		RemotePath string
	}{
		{
			Path:       "bar",
			RemotePath: "s3:foo/bar",
		},
		{
			Path:       "/bar",
			RemotePath: "s3:foo/bar",
		},
	}

	for _, test := range table {
		if p := l.RemotePath(test.Path); p != test.RemotePath {
			t.Error("expected", test.RemotePath, "got", p)
		}
	}
}

func TestDCLimitMarshalUnmarshalText(t *testing.T) {
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

	for _, test := range table {
		t.Run(test.Name, func(t *testing.T) {
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
				t.Errorf("got %s, expected %s", r, golden)
			}
		})
	}
}

func TestAggregateProgress(t *testing.T) {
	host1 := "host1"
	host2 := "host2"
	time1 := timeutc.Now().Add(-time.Hour)
	time2 := time1.Add(time.Minute)
	time3 := time1.Add(5 * time.Minute)
	run1 := &Run{
		Units: []Unit{
			{
				Keyspace: "ks",
				Tables:   []string{"table1", "table2"},
			},
		},
		DC:        []string{"dc1", "dc2"},
		StartTime: time1,
	}
	runNoUnits := &Run{
		DC:        []string{"dc3"},
		StartTime: time1,
	}
	table := []struct {
		Name        string
		Run         *Run
		RunProgress []*RunProgress
		Expected    string
	}{
		{
			Name:        "run with no progress",
			Run:         run1,
			RunProgress: nil,
			Expected:    "no_run_progress.golden.json",
		},
		{
			Name: "run with no units",
			Run:  runNoUnits,
			RunProgress: []*RunProgress{
				{},
			},
			Expected: "no_units.golden.json",
		},
		{
			Name: "run with success progress",
			Run:  run1,
			RunProgress: []*RunProgress{
				{
					Host:        host1,
					Unit:        0,
					TableName:   "table1",
					FileName:    "file1.f",
					StartedAt:   &time1,
					CompletedAt: &time2,
					Error:       "",
					Size:        10,
					Skipped:     10,
				},
				{
					Host:        host1,
					Unit:        0,
					TableName:   "table1",
					FileName:    "file12.f",
					StartedAt:   &time1,
					CompletedAt: &time2,
					Error:       "",
					Size:        7,
					Uploaded:    7,
				},
				{
					Host:        host1,
					Unit:        0,
					TableName:   "table2",
					FileName:    "file2.f",
					StartedAt:   &time1,
					CompletedAt: &time2,
					Error:       "",
					Size:        10,
					Uploaded:    5,
				},
				{
					Host:        host2,
					Unit:        0,
					TableName:   "table1",
					FileName:    "file1.f",
					StartedAt:   &time1,
					CompletedAt: &time3,
					Error:       "",
					Size:        10,
					Uploaded:    10,
				},
				{
					Host:        host2,
					Unit:        0,
					TableName:   "table2",
					FileName:    "file2.f",
					StartedAt:   &time1,
					CompletedAt: &time3,
					Error:       "",
					Size:        10,
					Uploaded:    3,
				},
			},
			Expected: "on_success.golden.json",
		},
		{
			Name: "run with error progress",
			Run:  run1,
			RunProgress: []*RunProgress{
				{
					Host:        host1,
					Unit:        0,
					TableName:   "table1",
					FileName:    "file1.f",
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
					FileName:    "file2.f",
					StartedAt:   &time1,
					CompletedAt: &time2,
					Error:       "error1",
					Size:        10,
					Uploaded:    5,
					Failed:      5,
				},
				{
					Host:        host1,
					Unit:        0,
					TableName:   "table2",
					FileName:    "file21.f",
					StartedAt:   &time1,
					CompletedAt: &time2,
					Error:       "error2",
					Size:        10,
					Uploaded:    5,
					Failed:      5,
				},
				{
					Host:        host2,
					Unit:        0,
					TableName:   "table1",
					FileName:    "file1.f",
					StartedAt:   &time1,
					CompletedAt: &time3,
					Error:       "",
					Size:        10,
					Uploaded:    10,
				},
				{
					Host:        host2,
					Unit:        0,
					TableName:   "table2",
					FileName:    "file2.f",
					StartedAt:   &time1,
					CompletedAt: &time3,
					Error:       "",
					Size:        10,
					Uploaded:    3,
				},
			},
			Expected: "on_error.golden.json",
		},
	}

	opts := cmp.Options{
		cmp.AllowUnexported(Progress{}, HostProgress{}, KeyspaceProgress{}, TableProgress{}),
		cmpopts.IgnoreUnexported(progress{}),
	}

	for _, test := range table {
		t.Run(test.Name, func(t *testing.T) {
			f, err := os.Open(path.Join("testdata/aggregate_progress", test.Expected))
			if err != nil {
				t.Fatal(err)
			}
			var expected Progress
			if err := json.NewDecoder(f).Decode(&expected); err != nil {
				t.Fatal(err)
			}
			f.Close()
			if diff := cmp.Diff(expected, aggregateProgress(test.Run, test.RunProgress), opts); diff != "" {
				t.Error(diff)
			}
		})
	}
}
