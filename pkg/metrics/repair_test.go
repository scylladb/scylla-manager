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

	t.Run("SetTaskProgress", func(t *testing.T) {
		vnode := uuid.MustParse("965f4f5c-c7d1-4ae6-b770-a2225df4ef49")
		tablet := uuid.MustParse("8fd16af1-815b-46db-bb2d-bd0a42ee9f92")
		m.SetTaskProgress(c, vnode, RepairTypeVnode, RepairMode("full"), 42)
		m.SetTaskProgress(c, tablet, RepairTypeTablet, RepairMode(""), 100)

		text := Dump(t, m.taskProgress)

		testutils.SaveGoldenTextFileIfNeeded(t, text)
		golden := testutils.LoadGoldenTextFile(t)
		if diff := cmp.Diff(text, golden); diff != "" {
			t.Error(diff)
		}
	})

	t.Run("ResetClusterMetrics keeps per task progress", func(t *testing.T) {
		// ResetClusterMetrics runs at the beginning of every repair run,
		// so it must not touch the progress of other repair tasks.
		other := uuid.MustParse("1b967567-8bc4-4407-9e1d-c7f37069415e")
		m.SetTaskProgress(c, other, RepairTypeVnode, RepairMode(""), 50)
		m.ResetClusterMetrics(c)

		if text := Dump(t, m.taskProgress); !strings.Contains(text, other.String()) {
			t.Errorf("expected task progress of %s to be kept, got %q", other, text)
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
