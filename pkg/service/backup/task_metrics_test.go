// Copyright (C) 2026 ScyllaDB

package backup

import (
	"math"
	"testing"

	"github.com/scylladb/scylla-manager/v3/pkg/metrics"
	"github.com/scylladb/scylla-manager/v3/pkg/util/uuid"
)

func TestFileBytesProgress(t *testing.T) {
	testCases := []struct {
		name  string
		bytes fileBytes
		want  float64
	}{
		{name: "nothing to back up", bytes: fileBytes{}, want: 0},
		{name: "not started", bytes: fileBytes{size: 100}, want: 0},
		{name: "half uploaded", bytes: fileBytes{size: 100, uploaded: 50}, want: 50},
		{name: "skipped counts as done", bytes: fileBytes{size: 100, uploaded: 20, skipped: 30}, want: 50},
		{name: "fully deduplicated", bytes: fileBytes{size: 100, skipped: 100}, want: 100},
		{name: "capped at 100", bytes: fileBytes{size: 100, uploaded: 120}, want: 100},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			if got := tc.bytes.progress(); math.Abs(got-tc.want) > 1e-9 {
				t.Fatalf("progress() = %v, expected %v", got, tc.want)
			}
		})
	}
}

func TestTaskMetricsAggregatorTotals(t *testing.T) {
	clusterID := uuid.MustParse("b703df56-c428-46a7-bfba-cfa6ee91b976")
	taskID := uuid.MustParse("965f4f5c-c7d1-4ae6-b770-a2225df4ef49")
	a := newTaskMetricsAggregator(clusterID, taskID, MethodRclone, metrics.NewBackupMetrics())

	// Two nodes, two tables each, reported repeatedly with absolute values.
	a.update("h1", "ks", "t1", fileBytes{size: 100})
	a.update("h1", "ks", "t2", fileBytes{size: 100})
	a.update("h2", "ks", "t1", fileBytes{size: 200})

	if a.total.size != 400 {
		t.Fatalf("total size = %d, expected 400", a.total.size)
	}
	if got := a.total.progress(); got != 0 {
		t.Fatalf("progress = %v, expected 0", got)
	}

	// Re-reporting the same table must not double count.
	a.update("h1", "ks", "t1", fileBytes{size: 100, uploaded: 50})
	a.update("h1", "ks", "t1", fileBytes{size: 100, uploaded: 100})
	if a.total.uploaded != 100 {
		t.Fatalf("total uploaded = %d, expected 100", a.total.uploaded)
	}
	if got := a.host["h1"].uploaded; got != 100 {
		t.Fatalf("h1 uploaded = %d, expected 100", got)
	}
	if got := a.host["h2"].uploaded; got != 0 {
		t.Fatalf("h2 uploaded = %d, expected 0", got)
	}

	// Finish everything, mixing uploaded and deduplicated bytes.
	a.update("h1", "ks", "t2", fileBytes{size: 100, skipped: 100})
	a.update("h2", "ks", "t1", fileBytes{size: 200, uploaded: 200})
	if got := a.total.progress(); math.Abs(got-100) > 1e-9 {
		t.Fatalf("progress = %v, expected 100", got)
	}
}

func TestBackupTypeFromMethod(t *testing.T) {
	testCases := []struct {
		method Method
		want   string
	}{
		{method: MethodRclone, want: metrics.BackupTypeRclone},
		{method: MethodNative, want: metrics.BackupTypeNative},
		{method: MethodAuto, want: metrics.BackupTypeAuto},
		{method: "", want: metrics.BackupTypeAuto},
	}

	for _, tc := range testCases {
		t.Run(string(tc.method), func(t *testing.T) {
			if got := backupTypeFromMethod(tc.method); got != tc.want {
				t.Fatalf("backupTypeFromMethod(%q) = %q, expected %q", tc.method, got, tc.want)
			}
		})
	}
}
