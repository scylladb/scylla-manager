// Copyright (C) 2017 ScyllaDB

package managerclient

import (
	"strings"
	"testing"
	"time"

	"github.com/go-openapi/strfmt"
	"github.com/scylladb/scylla-manager/v3/pkg/util/timeutc"
	"github.com/scylladb/scylla-manager/v3/pkg/util/uuid"
)

func TestTaskSplit(t *testing.T) {
	t.Parallel()

	table := []struct {
		S   string
		T   string
		ID  uuid.UUID
		Err string
	}{
		{
			S:  "repair/d7d4b241-f7fe-434e-bc8e-6185b30b078a",
			T:  "repair",
			ID: uuid.MustParse("d7d4b241-f7fe-434e-bc8e-6185b30b078a"),
		},
		{
			S: "repair",
			T: "repair",
		},
		{
			S:   "foo",
			T:   "foo",
			Err: "foo",
		},
	}

	for i, test := range table {
		tp, id, _, err := TaskSplit(test.S)
		if test.Err != "" {
			t.Log(err)
			if err == nil || !strings.Contains(err.Error(), test.Err) {
				t.Error(i, err)
			}
		} else if err != nil {
			t.Error(i, err)
		}

		if tp != test.T {
			t.Error(i, tp)
		}
		if id != test.ID {
			t.Error(i, id)
		}
	}
}

func TestUUIDFromLocation(t *testing.T) {
	t.Parallel()

	u0 := uuid.MustRandom()
	u1, err := uuidFromLocation("http://bla/bla/" + u0.String() + "?param=true")
	if err != nil {
		t.Fatal(err)
	}
	if u1 != u0 {
		t.Fatal(u1, u0)
	}
}

func TestFormatTimeZero(t *testing.T) {
	t.Parallel()

	if s := FormatTime(strfmt.DateTime(time.Time{})); s != "" {
		t.Error(s)
	}
}

func TestFormatTimeNonZero(t *testing.T) {
	t.Parallel()

	tz, _ := timeutc.Now().Local().Zone()

	if s := FormatTime(strfmt.DateTime(timeutc.Now())); !strings.Contains(s, tz) {
		t.Error(s)
	}
}

func TestFormatTables(t *testing.T) {
	t.Parallel()

	table := []struct {
		Name      string
		Threshold int
		Tables    []string
		AllTables bool
		Golden    string
	}{
		{
			Name:      "empty with threshold",
			Threshold: 2,
			Golden:    "(0 tables)",
		},
		{
			Name:      "empty unlimited",
			Threshold: -1,
			Golden:    "(0 tables)",
		},
		{
			Name:      "unlimited",
			Threshold: -1,
			Tables:    []string{"a", "b"},
			Golden:    "(a, b)",
		},
		{
			Name:   "one table",
			Tables: []string{"a"},
			Golden: "(1 table)",
		},
		{
			Name:   "no threshold",
			Tables: []string{"a", "b"},
			Golden: "(2 tables)",
		},
		{
			Name:      "above threshold",
			Threshold: 1,
			Tables:    []string{"a", "b"},
			Golden:    "(2 tables)",
		},
		{
			Name:      "below threshold",
			Threshold: 1,
			Tables:    []string{"a"},
			Golden:    "(a)",
		},
		{
			Name:      "all tables above threshold",
			Threshold: 1,
			Tables:    []string{"a", "b"},
			AllTables: true,
			Golden:    "all (2 tables)",
		},
		{
			Name:      "all tables below threshold",
			Threshold: 1,
			Tables:    []string{"a"},
			AllTables: true,
			Golden:    "all (a)",
		},
	}

	for i := range table {
		test := table[i]

		t.Run(test.Name, func(t *testing.T) {
			t.Parallel()

			if s := FormatTables(test.Threshold, test.Tables, test.AllTables); s != test.Golden {
				t.Errorf("FormatTables() expected %s got %s", test.Golden, s)
			}
		})
	}
}

func TestFormatUploadProgress(t *testing.T) {
	t.Parallel()

	table := []struct {
		Name     string
		Size     int64
		Uploaded int64
		Skipped  int64
		Failed   int64
		Golden   string
	}{
		{
			Name:     "everything uploaded",
			Size:     10,
			Uploaded: 10,
			Golden:   "100%",
		},
		{
			Name:    "everything skipped",
			Size:    10,
			Skipped: 10,
			Golden:  "100%",
		},
		{
			Name:   "everything failed",
			Size:   10,
			Failed: 10,
			Golden: "0%/100%",
		},
		{
			Name:     "partial failure complete",
			Size:     10,
			Uploaded: 5,
			Skipped:  3,
			Failed:   2,
			Golden:   "80%/20%",
		},
		{
			Name:     "partial failure not-complete",
			Size:     10,
			Uploaded: 5,
			Skipped:  3,
			Failed:   1,
			Golden:   "80%/10%",
		},
	}

	for i := range table {
		test := table[i]

		t.Run(test.Name, func(t *testing.T) {
			t.Parallel()

			if s := FormatUploadProgress(test.Size, test.Uploaded, test.Skipped, test.Failed); s != test.Golden {
				t.Errorf("FormatUploadProgress() expected %s got %s", test.Golden, s)
			}
		})
	}
}

func TestFormatRepairProgress(t *testing.T) {
	t.Parallel()

	table := []struct {
		Name    string
		Total   int64
		Success int64
		Skipped int64
		Failed  int64
		Golden  string
	}{
		{
			Name:    "zero",
			Total:   0,
			Success: 0,
			Golden:  "-",
		},
		{
			Name:    "everything uploaded",
			Total:   1536,
			Success: 1536,
			Golden:  "100%",
		},
		{
			Name:   "no progress",
			Total:  1536,
			Golden: "0%",
		},
		{
			Name:   "everything failed",
			Total:  1536,
			Failed: 1536,
			Golden: "0%/100%",
		},
		{
			Name:    "partial failure complete",
			Total:   1536,
			Success: 1228,
			Failed:  308,
			Golden:  "79%/21%",
		},
		{
			Name:    "partial failure not-complete",
			Total:   1536,
			Success: 1229,
			Failed:  154,
			Golden:  "80%/11%",
		},
	}

	for i := range table {
		test := table[i]

		t.Run(test.Name, func(t *testing.T) {
			t.Parallel()

			if s := FormatRepairProgress(test.Total, test.Success, test.Failed); s != test.Golden {
				t.Errorf("FormatRepairProgress() expected %s got %s", test.Golden, s)
			}
		})
	}
}

func TestFormatIntensity(t *testing.T) {
	table := []struct {
		Intensity float64
		Golden    string
	}{
		{
			Intensity: -1,
			Golden:    "max",
		},
		{
			Intensity: 1,
			Golden:    "1",
		},
		{
			Intensity: 0.1111,
			Golden:    "0.11",
		},
	}

	for i := range table {
		test := table[i]
		t.Run(test.Golden, func(t *testing.T) {
			t.Parallel()

			if s := FormatIntensity(test.Intensity); s != test.Golden {
				t.Errorf("FormatIntensity() expected %s got %s", test.Golden, s)
			}
		})
	}
}
