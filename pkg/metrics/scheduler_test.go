// Copyright (C) 2017 ScyllaDB

package metrics

import (
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
		m.EndRun(c, p, t0, "DONE", 1645517563, 1645517600)
		m.EndRun(c, p, t1, "ERROR", 1645517563, 1645517700)

		text := Dump(t, m.runIndicator, m.runsTotal, m.lastSuccess, m.taskState,
			m.taskRunStartSeconds, m.taskLastSuccessSecond)

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

	t.Run("tablet repair is reported as repair", func(t *testing.T) {
		m := NewSchedulerMetrics()
		m.BeginRun(c, "tablet_repair", taskID, 1645517563)

		text := Dump(t, m.taskState)

		testutils.SaveGoldenTextFileIfNeeded(t, text)
		golden := testutils.LoadGoldenTextFile(t)
		if diff := cmp.Diff(text, golden); diff != "" {
			t.Error(diff)
		}
	})

	t.Run("state is latched after the run", func(t *testing.T) {
		m := NewSchedulerMetrics()
		m.BeginRun(c, "backup", taskID, 1645517563)
		m.EndRun(c, "backup", taskID, "ERROR", 1645517563, 1645517700)

		text := Dump(t, m.taskState)

		testutils.SaveGoldenTextFileIfNeeded(t, text)
		golden := testutils.LoadGoldenTextFile(t)
		if diff := cmp.Diff(text, golden); diff != "" {
			t.Error(diff)
		}
	})

	t.Run("restored from the last task status", func(t *testing.T) {
		m := NewSchedulerMetrics()
		m.InitTaskState(c, "backup", taskID, "ERROR")

		text := Dump(t, m.taskState)

		testutils.SaveGoldenTextFileIfNeeded(t, text)
		golden := testutils.LoadGoldenTextFile(t)
		if diff := cmp.Diff(text, golden); diff != "" {
			t.Error(diff)
		}
	})

	t.Run("last success is restored and reports the run end time", func(t *testing.T) {
		m := NewSchedulerMetrics()
		m.InitTaskLastSuccess(c, "tablet_repair", taskID, 1645517700)
		m.BeginRun(c, "backup", taskID, 1645600000)
		m.EndRun(c, "backup", taskID, "DONE", 1645600000, 1645700000)

		text := Dump(t, m.taskRunStartSeconds, m.taskLastSuccessSecond)

		testutils.SaveGoldenTextFileIfNeeded(t, text)
		golden := testutils.LoadGoldenTextFile(t)
		if diff := cmp.Diff(text, golden); diff != "" {
			t.Error(diff)
		}
	})

	t.Run("not restored for a task that never finished a run", func(t *testing.T) {
		m := NewSchedulerMetrics()
		m.InitTaskState(c, "backup", taskID, "NEW")
		m.InitTaskState(c, "backup", taskID, "RUNNING")

		if text := Dump(t, m.taskState); text != "" {
			t.Errorf("expected no task state, got %q", text)
		}
	})
}
