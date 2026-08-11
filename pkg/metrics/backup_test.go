// Copyright (C) 2017 ScyllaDB

package metrics

import (
	"fmt"
	"testing"

	"github.com/google/go-cmp/cmp"

	"github.com/scylladb/scylla-manager/v3/pkg/testutils"
	"github.com/scylladb/scylla-manager/v3/pkg/util/uuid"
)

func TestBackupMetrics(t *testing.T) {
	m := NewBackupMetrics()
	c := uuid.MustParse("b703df56-c428-46a7-bfba-cfa6ee91b976")

	t.Run("SetSnapshot", func(t *testing.T) {
		m.SetSnapshot(c, "k0", "h", false)
		m.SetSnapshot(c, "k1", "h", true)

		text := Dump(t, m.snapshot)

		testutils.SaveGoldenTextFileIfNeeded(t, text)
		golden := testutils.LoadGoldenTextFile(t)
		if diff := cmp.Diff(text, golden); diff != "" {
			t.Error(diff)
		}
	})

	t.Run("SetFilesProgress", func(t *testing.T) {
		m.SetFilesProgress(c, "k", "t", "h", 10, 5, 3, 2, 7, 3)

		text := Dump(t, m.filesSizeBytes, m.filesUploadedBytes, m.filesSkippedBytes, m.filesFailedBytes, m.filesCount, m.filesSkippedCount)
		fmt.Println(text)
		testutils.SaveGoldenTextFileIfNeeded(t, text)
		golden := testutils.LoadGoldenTextFile(t)
		if diff := cmp.Diff(text, golden); diff != "" {
			t.Error(diff)
		}
	})

	t.Run("SetPurgeFiles", func(t *testing.T) {
		m.SetPurgeFiles(c, "h", 2, 1)

		text := Dump(t, m.purgeFiles, m.purgeDeletedFiles)

		testutils.SaveGoldenTextFileIfNeeded(t, text)
		golden := testutils.LoadGoldenTextFile(t)
		if diff := cmp.Diff(text, golden); diff != "" {
			t.Error(diff)
		}
	})

	t.Run("SetVersionedFilesCount", func(t *testing.T) {
		m.SetVersionedFilesCount(c, "n", "k", "t", 2)

		text := Dump(t, m.versionedFilesCount)

		testutils.SaveGoldenTextFileIfNeeded(t, text)
		golden := testutils.LoadGoldenTextFile(t)
		if diff := cmp.Diff(text, golden); diff != "" {
			t.Error(diff)
		}
	})

	t.Run("IncreaseEventBasedHolds", func(t *testing.T) {
		m.IncreaseEventBasedHolds(c, "n", "k", "t", "h", true, 2)
		m.IncreaseEventBasedHolds(c, "n", "k", "t", "h", true, 3)
		m.IncreaseEventBasedHolds(c, "n", "k", "t", "h", false, 1)

		text := Dump(t, m.setEventBasedHolds, m.removedEventBasedHolds)

		testutils.SaveGoldenTextFileIfNeeded(t, text)
		golden := testutils.LoadGoldenTextFile(t)
		if diff := cmp.Diff(text, golden); diff != "" {
			t.Error(diff)
		}
	})

	t.Run("ResetClusterMetrics", func(t *testing.T) {
		// Use a dedicated metrics instance, so that this subtest
		// doesn't depend on series created by the other subtests.
		m := NewBackupMetrics()
		c2 := uuid.MustParse("400d09d8-b3ce-4023-102c-6912671b051a")
		// Set all metrics for two clusters
		for _, cluster := range []uuid.UUID{c, c2} {
			m.SetSnapshot(cluster, "k", "h", true)
			m.SetFilesProgress(cluster, "k", "t", "h", 10, 5, 3, 2, 7, 3)
			m.SetPurgeFiles(cluster, "h", 2, 1)
			m.IncreaseRetentionLockedFiles(cluster, "k", "t", "h", 5)
			m.SetVersionedFilesCount(cluster, "n", "k", "t", 2)
			m.IncreaseEventBasedHolds(cluster, "n", "k", "t", "h", true, 2)
			m.IncreaseEventBasedHolds(cluster, "n", "k", "t", "h", false, 1)
		}
		// Reset all metrics for the first cluster
		m.ResetClusterMetrics(c)
		// Expect:
		// - second cluster metrics to be intact
		// - older first cluster metrics to be set to -1
		// - newer first cluster metrics to be deleted
		text := Dump(t, m.all()...)

		testutils.SaveGoldenTextFileIfNeeded(t, text)
		golden := testutils.LoadGoldenTextFile(t)
		if diff := cmp.Diff(text, golden); diff != "" {
			t.Error(diff)
		}
	})
}
