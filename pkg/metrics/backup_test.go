// Copyright (C) 2017 ScyllaDB

package metrics

import (
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
		m.SetFilesProgress(c, "k", "t", "h", 10, 5, 3, 2)

		text := Dump(t, m.filesSizeBytes, m.filesUploadedBytes, m.filesSkippedBytes, m.filesFailedBytes)

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
}
