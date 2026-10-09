// Copyright (C) 2017 ScyllaDB

package metrics

import (
	"strings"
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/scylladb/scylla-manager/v3/pkg/testutils"
	"github.com/scylladb/scylla-manager/v3/pkg/util/uuid"
)

func TestRepairMetrics(t *testing.T) {
	m := NewRepairMetrics()
	c := uuid.MustParse("b703df56-c428-46a7-bfba-cfa6ee91b976")

	t.Run("SetTokenRanges", func(t *testing.T) {
		m.SetTokenRanges(c, "k", "t", "h", 3, 2, 1)

		text := Dump(t, m.tokenRangesTotal, m.tokenRangesSuccess, m.tokenRangesError)

		testutils.SaveGoldenTextFileIfNeeded(t, text)
		golden := testutils.LoadGoldenTextFile(t)
		if diff := cmp.Diff(text, golden); diff != "" {
			t.Error(diff)
		}
	})

	t.Run("SetTaskProperties", func(t *testing.T) {
		task := uuid.MustParse("965f4f5c-c7d1-4ae6-b770-a2225df4ef49")
		m.SetTaskProperties(c, task, RepairTaskProperties{
			Keyspace:            "*,!system_traces",
			DC:                  "all",
			KeyspaceReplication: "all",
			Host:                "all",
			FailFast:            "false",
		})

		text := Dump(t, m.taskProperties)

		testutils.SaveGoldenTextFileIfNeeded(t, text)
		golden := testutils.LoadGoldenTextFile(t)
		if diff := cmp.Diff(text, golden); diff != "" {
			t.Error(diff)
		}
	})

	t.Run("SetProgress", func(t *testing.T) {
		task := uuid.MustParse("965f4f5c-c7d1-4ae6-b770-a2225df4ef49")
		other := uuid.MustParse("8fd16af1-815b-46db-bb2d-bd0a42ee9f92")
		m.SetProgress(c, task, 42)
		m.SetProgress(c, other, 100)

		text := Dump(t, m.progress)

		testutils.SaveGoldenTextFileIfNeeded(t, text)
		golden := testutils.LoadGoldenTextFile(t)
		if diff := cmp.Diff(text, golden); diff != "" {
			t.Error(diff)
		}
	})

	t.Run("ResetMetrics keeps the progress of other tasks", func(t *testing.T) {
		// ResetMetrics runs at the beginning of every repair run, so it must
		// not touch the progress of other repair tasks, only zero the one of
		// the task that is about to run.
		task := uuid.MustParse("965f4f5c-c7d1-4ae6-b770-a2225df4ef49")
		other := uuid.MustParse("1b967567-8bc4-4407-9e1d-c7f37069415e")
		m.SetProgress(c, task, 42)
		m.SetProgress(c, other, 50)
		m.ResetMetrics(c, task)

		text := Dump(t, m.progress)
		if !strings.Contains(text, other.String()+"\"} 50") {
			t.Errorf("expected progress of %s to be kept, got %q", other, text)
		}
		if !strings.Contains(text, task.String()+"\"} 0") {
			t.Errorf("expected progress of %s to be reset, got %q", task, text)
		}
	})

	t.Run("AddSubJob", func(t *testing.T) {
		m.AddJob(c, "h", 10)
		m.AddJob(c, "h", 10)
		m.SubJob(c, "h", 10)

		text := Dump(t, m.inFlightJobs, m.inFlightTokenRanges)

		testutils.SaveGoldenTextFileIfNeeded(t, text)
		golden := testutils.LoadGoldenTextFile(t)
		if diff := cmp.Diff(text, golden); diff != "" {
			t.Error(diff)
		}
	})
}
