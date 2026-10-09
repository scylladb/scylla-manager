// Copyright (C) 2017 ScyllaDB

package metrics

import (
	"strings"
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/scylladb/scylla-manager/v3/pkg/testutils"
	"github.com/scylladb/scylla-manager/v3/pkg/util/uuid"
)

func TestSchedulerMetrics(t *testing.T) {
	m := NewSchedulerMetrics()
	c := uuid.MustParse("b703df56-c428-46a7-bfba-cfa6ee91b976")
	p := "backup"
	t0 := uuid.MustParse("965f4f5c-c7d1-4ae6-b770-a2225df4ef49")
	t1 := uuid.MustParse("8fd16af1-815b-46db-bb2d-bd0a42ee9f92")
	t2 := uuid.MustParse("1b967567-8bc4-4407-9e1d-c7f37069415e")

	t.Run("Init", func(t *testing.T) {
		m.Init(c, p, t0, "DONE", "ERROR")

		text := Dump(t, m.runIndicator, m.runsTotal)

		testutils.SaveGoldenTextFileIfNeeded(t, text)
		golden := testutils.LoadGoldenTextFile(t)
		if diff := cmp.Diff(text, golden); diff != "" {
			t.Error(diff)
		}
	})

	t.Run("BeginEndRun", func(t *testing.T) {
		m.BeginRun(c, p, t0, 1645517563)
		m.BeginRun(c, p, t1, 1645517563)
		m.BeginRun(c, p, t2, 1645517563)
		m.EndRun(c, p, t0, "DONE", 1645517563)
		m.SetTaskState(c, p, t0, TaskStateDone)
		m.EndRun(c, p, t1, "ERROR", 1645517563)
		m.SetTaskState(c, p, t1, TaskStateError)

		text := Dump(t, m.runIndicator, m.runsTotal, m.lastSuccess, m.taskState,
			m.taskRunStartSeconds)

		testutils.SaveGoldenTextFileIfNeeded(t, text)
		golden := testutils.LoadGoldenTextFile(t)
		if diff := cmp.Diff(text, golden); diff != "" {
			t.Error(diff)
		}
	})

	t.Run("DeleteTaskMetrics", func(t *testing.T) {
		m := NewSchedulerMetrics()
		m.Init(c, p, t0, "DONE", "ERROR")
		m.Init(c, p, t1, "DONE", "ERROR")
		m.BeginRun(c, p, t0, 1645517563)
		m.BeginRun(c, p, t1, 1645517563)
		m.EndRun(c, p, t0, "DONE", 1645517563)
		m.EndRun(c, p, t1, "DONE", 1645517563)
		m.DeleteTaskMetrics(t1)

		text := Dump(t, m.runIndicator, m.runsTotal, m.lastSuccess)

		testutils.SaveGoldenTextFileIfNeeded(t, text)
		golden := testutils.LoadGoldenTextFile(t)
		if diff := cmp.Diff(text, golden); diff != "" {
			t.Error(diff)
		}
	})

	t.Run("last success reports the run start time", func(t *testing.T) {
		m := NewSchedulerMetrics()
		m.BeginRun(c, p, t0, 1645600000)
		m.EndRun(c, p, t0, "DONE", 1645600000)

		text := Dump(t, m.taskRunStartSeconds, m.lastSuccess)

		testutils.SaveGoldenTextFileIfNeeded(t, text)
		golden := testutils.LoadGoldenTextFile(t)
		if diff := cmp.Diff(text, golden); diff != "" {
			t.Error(diff)
		}
	})
}

func TestSchedulerMetricsTaskState(t *testing.T) {
	c := uuid.MustParse("b703df56-c428-46a7-bfba-cfa6ee91b976")
	taskID := uuid.MustParse("965f4f5c-c7d1-4ae6-b770-a2225df4ef49")

	t.Run("tablet repair keeps its own task type", func(t *testing.T) {
		m := NewSchedulerMetrics()
		m.BeginRun(c, "tablet_repair", taskID, 1645517563)

		if text := Dump(t, m.taskState); !strings.Contains(text, `type="tablet_repair"`) {
			t.Errorf("expected the tablet repair task type, got %q", text)
		}
	})

	t.Run("state is latched after the run", func(t *testing.T) {
		m := NewSchedulerMetrics()
		m.BeginRun(c, "backup", taskID, 1645517563)
		m.SetTaskState(c, "backup", taskID, TaskStateError)

		text := Dump(t, m.taskState)

		testutils.SaveGoldenTextFileIfNeeded(t, text)
		golden := testutils.LoadGoldenTextFile(t)
		if diff := cmp.Diff(text, golden); diff != "" {
			t.Error(diff)
		}
	})

	t.Run("task properties report the schedule and the name", func(t *testing.T) {
		m := NewSchedulerMetrics()
		m.SetTaskProperties(c, "backup", taskID, "daily", "0 23 * * *")

		text := Dump(t, m.taskProperties)

		testutils.SaveGoldenTextFileIfNeeded(t, text)
		golden := testutils.LoadGoldenTextFile(t)
		if diff := cmp.Diff(text, golden); diff != "" {
			t.Error(diff)
		}
	})

	t.Run("task properties fall back to the task ID for an unnamed task", func(t *testing.T) {
		m := NewSchedulerMetrics()
		m.SetTaskProperties(c, "repair", taskID, "", "1d")

		// Two unnamed tasks must stay apart, otherwise a panel grouping by
		// name merges them into one row.
		if text := Dump(t, m.taskProperties); !strings.Contains(text, `name="`+taskID.String()+`"`) {
			t.Errorf("expected the task ID as name, got %q", text)
		}
	})

	t.Run("task properties replace the series when the task is renamed", func(t *testing.T) {
		m := NewSchedulerMetrics()
		m.SetTaskProperties(c, "repair", taskID, "old", "1d")
		m.SetTaskProperties(c, "repair", taskID, "new", "1d")

		text := Dump(t, m.taskProperties)
		if strings.Contains(text, `name="old"`) {
			t.Errorf("stale series kept: %q", text)
		}
		if !strings.Contains(text, `name="new"`) {
			t.Errorf("new series missing: %q", text)
		}
	})
}
