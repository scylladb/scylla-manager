// Copyright (C) 2017 ScyllaDB

package metrics

import (
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
