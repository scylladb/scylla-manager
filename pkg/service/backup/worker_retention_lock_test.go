// Copyright (C) 2026 ScyllaDB

package backup_test

import (
	"testing"
	"time"

	"github.com/scylladb/scylla-manager/backupspec"
	"github.com/scylladb/scylla-manager/v3/pkg/service/backup"
)

func TestRetentionLockUntil(t *testing.T) {
	t.Parallel()

	snapshotTime := time.Date(2026, 1, 15, 10, 30, 0, 0, time.UTC)
	snapshotTag := backupspec.SnapshotTagAt(snapshotTime)

	testCases := []struct {
		name          string
		snapshotTag   string
		retentionDays int
		expected      time.Time
		err           bool
	}{
		{
			name:          "valid tag with 1 day retention",
			snapshotTag:   snapshotTag,
			retentionDays: 1,
			expected:      snapshotTime.Add(24 * time.Hour),
		},
		{
			name:          "valid tag with 7 days retention",
			snapshotTag:   snapshotTag,
			retentionDays: 7,
			expected:      snapshotTime.Add(7 * 24 * time.Hour),
		},
		{
			name:          "valid tag with 30 days retention",
			snapshotTag:   snapshotTag,
			retentionDays: 30,
			expected:      snapshotTime.Add(30 * 24 * time.Hour),
		},
		{
			name:          "invalid snapshot tag",
			snapshotTag:   "invalid_tag",
			retentionDays: 1,
			err:           true,
		},
		{
			name:          "empty snapshot tag",
			snapshotTag:   "",
			retentionDays: 1,
			err:           true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			got, err := backup.RetentionLockUntil(tc.snapshotTag, tc.retentionDays)
			if tc.err {
				if err == nil {
					t.Fatal("expected error, got nil")
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if !got.Equal(tc.expected) {
				t.Fatalf("got %v, expected %v", got, tc.expected)
			}
		})
	}
}
