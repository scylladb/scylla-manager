// Copyright (C) 2026 ScyllaDB

package backup

import (
	"fmt"
	"math"
	"sync"
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
	a := newTaskMetricsAggregator(clusterID, taskID, metrics.NewBackupMetrics())

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

	// Finish everything, mixing uploaded and deduplicated bytes.
	a.update("h1", "ks", "t2", fileBytes{size: 100, skipped: 100})
	a.update("h2", "ks", "t1", fileBytes{size: 200, uploaded: 200})
	if got := a.total.progress(); math.Abs(got-100) > 1e-9 {
		t.Fatalf("progress = %v, expected 100", got)
	}
}

func TestTaskMetricsAggregatorConcurrentUpdates(t *testing.T) {
	clusterID := uuid.MustParse("b703df56-c428-46a7-bfba-cfa6ee91b976")
	taskID := uuid.MustParse("965f4f5c-c7d1-4ae6-b770-a2225df4ef49")
	a := newTaskMetricsAggregator(clusterID, taskID, metrics.NewBackupMetrics())

	// Backup progress is reported by many table uploads at once. Every table
	// is owned by a single goroutine, so the totals are deterministic no
	// matter how the updates interleave.
	const (
		hosts  = 4
		tables = 8
		steps  = 25
	)
	var wg sync.WaitGroup
	for h := range hosts {
		for tab := range tables {
			wg.Add(1)
			go func() {
				defer wg.Done()
				host := fmt.Sprintf("h%d", h)
				table := fmt.Sprintf("t%d", tab)
				for s := 1; s <= steps; s++ {
					a.update(host, "ks", table, fileBytes{size: steps, uploaded: int64(s)})
				}
			}()
		}
	}
	wg.Wait()

	if want := int64(hosts * tables * steps); a.total.size != want {
		t.Fatalf("total size = %d, expected %d", a.total.size, want)
	}
	if want := int64(hosts * tables * steps); a.total.uploaded != want {
		t.Fatalf("total uploaded = %d, expected %d", a.total.uploaded, want)
	}
	if got := a.total.progress(); math.Abs(got-100) > 1e-9 {
		t.Fatalf("progress = %v, expected 100", got)
	}
}

func TestTaskPropertiesMetric(t *testing.T) {
	expand := func(p taskProperties) taskProperties {
		p.expandDefaultTaskProperties()
		return p
	}
	ptr := func(i int) *int { return &i }

	t.Run("unset properties report the service defaults", func(t *testing.T) {
		got := taskPropertiesMetric(expand(taskProperties{}))
		want := metrics.BackupTaskProperties{
			Keyspace:          "all",
			DC:                "all",
			Location:          "",
			Retention:         "3",
			RetentionDays:     "0",
			Method:            string(MethodRclone),
			PurgeOnly:         "false",
			SkipSchema:        "false",
			RetentionLockMode: string(RetentionLockDisabled),
		}
		if got != want {
			t.Errorf("got  %+v\nwant %+v", got, want)
		}
	})

	t.Run("configuring only retention days leaves retention at 0", func(t *testing.T) {
		got := taskPropertiesMetric(expand(taskProperties{RetentionDays: ptr(30)}))
		if got.Retention != "0" || got.RetentionDays != "30" {
			t.Errorf("retention = %q, retention days = %q", got.Retention, got.RetentionDays)
		}
	})

	t.Run("configured properties win", func(t *testing.T) {
		got := taskPropertiesMetric(expand(taskProperties{
			Keyspace:   []string{"ks", "!ks.tbl"},
			DC:         []string{"dc1"},
			Method:     MethodNative,
			SkipSchema: true,
		}))
		if got.Keyspace != "ks,!ks.tbl" || got.DC != "dc1" ||
			got.Method != string(MethodNative) || got.SkipSchema != "true" {
			t.Errorf("got %+v", got)
		}
	})
}
