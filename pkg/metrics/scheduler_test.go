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
		m.BeginRun(c, p, t0)
		m.BeginRun(c, p, t1)
		m.BeginRun(c, p, t2)
		m.EndRun(c, p, t0, "DONE", 1645517563)
		m.EndRun(c, p, t1, "ERROR", 1645517563)

		text := Dump(t, m.runIndicator, m.runsTotal, m.lastSuccess)

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
		m.BeginRun(c, p, t0)
		m.BeginRun(c, p, t1)
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

	t.Run("DeleteClusterMetrics", func(t *testing.T) {
		m := NewSchedulerMetrics()
		c1 := uuid.MustParse("3c4a8b40-9a2a-4f7c-8b2e-6f1d2f5b9e01")
		for _, cluster := range []uuid.UUID{c, c1} {
			m.Init(cluster, p, t0, "DONE", "ERROR")
			m.BeginRun(cluster, p, t0)
			m.EndRun(cluster, p, t0, "DONE", 1645517563)
			m.Suspend(cluster)
		}
		m.DeleteClusterMetrics(c)

		text := Dump(t, m.all()...)

		testutils.SaveGoldenTextFileIfNeeded(t, text)
		golden := testutils.LoadGoldenTextFile(t)
		if diff := cmp.Diff(text, golden); diff != "" {
			t.Error(diff)
		}
	})
}
