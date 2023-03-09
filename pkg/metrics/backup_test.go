// Copyright (C) 2017 ScyllaDB

package metrics

import (
	"fmt"
	"strings"
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/scylladb/scylla-manager/v3/pkg/testutils"
	"github.com/scylladb/scylla-manager/v3/pkg/util/uuid"
)

func TestBackupMetrics(t *testing.T) {
	m := NewBackupMetrics()
	c := uuid.MustParse("b703df56-c428-46a7-bfba-cfa6ee91b976")

	t.Run("SetSnapshot", func(t *testing.T) {
		m.Backup.SetSnapshot(c, "k0", "h", false)
		m.Backup.SetSnapshot(c, "k1", "h", true)

		text := Dump(t, m.Backup.snapshot)

		testutils.SaveGoldenTextFileIfNeeded(t, text)
		golden := testutils.LoadGoldenTextFile(t)
		if diff := cmp.Diff(text, golden); diff != "" {
			t.Error(diff)
		}
	})

	t.Run("SetFilesProgress", func(t *testing.T) {
		m.Backup.SetFilesProgress(c, "k", "t", "h", 10, 5, 3, 2)

		text := Dump(t, m.Backup.filesSizeBytes, m.Backup.filesUploadedBytes, m.Backup.filesSkippedBytes, m.Backup.filesFailedBytes)
		fmt.Println(text)
		testutils.SaveGoldenTextFileIfNeeded(t, text)
		golden := testutils.LoadGoldenTextFile(t)
		if diff := cmp.Diff(text, golden); diff != "" {
			t.Error(diff)
		}
	})

	t.Run("SetPurgeFiles", func(t *testing.T) {
		m.Backup.SetPurgeFiles(c, "h", 2, 1)

		text := Dump(t, m.Backup.purgeFiles, m.Backup.purgeDeletedFiles)

		testutils.SaveGoldenTextFileIfNeeded(t, text)
		golden := testutils.LoadGoldenTextFile(t)
		if diff := cmp.Diff(text, golden); diff != "" {
			t.Error(diff)
		}
	})
}

func TestRestoreMetrics(t *testing.T) {
	m := NewBackupMetrics()
	c := uuid.MustParse("b703df56-c428-46a7-bfba-cfa6ee91b976")

	t.Run("SetOrUpdateFilesSize", func(t *testing.T) {
		m.Restore.SetFilesSize(c, "m", "k", "t", 2)
		text1 := fmt.Sprintf("After: set(2)\n%s", Dump(t, m.Restore.filesSizeBytes))

		m.Restore.UpdateFilesSize(c, "m", "k", "t", 3)
		text2 := fmt.Sprintf("After: update(3)\n%s", Dump(t, m.Restore.filesSizeBytes))

		m.Restore.SetFilesSize(c, "m", "k", "t", 7)
		text3 := fmt.Sprintf("After: set(7)\n%s", Dump(t, m.Restore.filesSizeBytes))

		text := strings.Join([]string{text1, text2, text3}, "\n")

		testutils.SaveGoldenTextFileIfNeeded(t, text)
		golden := testutils.LoadGoldenTextFile(t)
		if diff := cmp.Diff(text, golden); diff != "" {
			t.Error(diff)
		}
	})

	t.Run("UpdateFilesProgress", func(t *testing.T) {
		m.Restore.UpdateFilesProgress(c, "m", "k", "t", 10, 5, 3)

		text := Dump(t, m.Restore.filesDownloadedBytes, m.Restore.filesSkippedBytes, m.Restore.filesFailedBytes)

		testutils.SaveGoldenTextFileIfNeeded(t, text)
		golden := testutils.LoadGoldenTextFile(t)
		if diff := cmp.Diff(text, golden); diff != "" {
			t.Error(diff)
		}
	})

	t.Run("UpdateRestoreProgress", func(t *testing.T) {
		m.Restore.UpdateRestoreProgress(c, "m", "k", "t", 2)

		text := Dump(t, m.Restore.filesRestoredBytes)

		testutils.SaveGoldenTextFileIfNeeded(t, text)
		golden := testutils.LoadGoldenTextFile(t)
		if diff := cmp.Diff(text, golden); diff != "" {
			t.Error(diff)
		}
	})
}
