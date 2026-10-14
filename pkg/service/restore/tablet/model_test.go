// Copyright (C) 2026 ScyllaDB

package tablet

import (
	"testing"
	"time"

	"github.com/scylladb/scylla-manager/v3/pkg/util/timeutc"
)

func TestRunProgressIsSuccess(t *testing.T) {
	now := timeutc.Now()

	testCases := []struct {
		name      string
		pr        RunProgress
		isSuccess bool
	}{
		{
			name:      "zero value",
			pr:        RunProgress{},
			isSuccess: false,
		},
		{
			name:      "started, task not scheduled yet",
			pr:        RunProgress{StartedAt: now},
			isSuccess: false,
		},
		{
			name:      "task scheduled, outcome unknown (e.g. SM crash)",
			pr:        RunProgress{StartedAt: now, Host: "h", ScyllaTaskID: "task"},
			isSuccess: false,
		},
		{
			name:      "completed without error",
			pr:        RunProgress{StartedAt: now, Host: "h", ScyllaTaskID: "task", CompletedAt: now.Add(time.Minute)},
			isSuccess: true,
		},
		{
			name:      "completed with error",
			pr:        RunProgress{StartedAt: now, Host: "h", ScyllaTaskID: "task", CompletedAt: now.Add(time.Minute), Error: "task failed"},
			isSuccess: false,
		},
		{
			name:      "error recorded without completion timestamp",
			pr:        RunProgress{StartedAt: now, Host: "h", ScyllaTaskID: "task", Error: "ctx cancelled"},
			isSuccess: false,
		},
		{
			name:      "error recorded before scheduling the task",
			pr:        RunProgress{StartedAt: now, Error: "schedule failed"},
			isSuccess: false,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			if got := tc.pr.isSuccess(); got != tc.isSuccess {
				t.Errorf("isSuccess() = %v, expected %v", got, tc.isSuccess)
			}
		})
	}
}

func TestRunProgressRestoredBytes(t *testing.T) {
	now := timeutc.Now()

	testCases := []struct {
		name     string
		pr       RunProgress
		restored int64
	}{
		{
			name:     "task not scheduled yet",
			pr:       RunProgress{Size: 100},
			restored: 0,
		},
		{
			name:     "task started without progress",
			pr:       RunProgress{Size: 100, TotalSSTables: 4},
			restored: 0,
		},
		{
			name:     "task in progress",
			pr:       RunProgress{Size: 100, RestoredSSTables: 1, TotalSSTables: 4},
			restored: 25,
		},
		{
			name:     "task in progress with rounding",
			pr:       RunProgress{Size: 100, RestoredSSTables: 1, TotalSSTables: 3},
			restored: 33,
		},
		{
			name:     "task done",
			pr:       RunProgress{Size: 100, RestoredSSTables: 4, TotalSSTables: 4, CompletedAt: now},
			restored: 100,
		},
		{
			name:     "task done with stale sstable counters",
			pr:       RunProgress{Size: 100, RestoredSSTables: 3, TotalSSTables: 4, CompletedAt: now},
			restored: 100,
		},
		{
			name:     "task failed",
			pr:       RunProgress{Size: 100, RestoredSSTables: 2, TotalSSTables: 4, CompletedAt: now, Error: "task failed"},
			restored: 50,
		},
		{
			name:     "restored exceeds total",
			pr:       RunProgress{Size: 100, RestoredSSTables: 5, TotalSSTables: 4},
			restored: 100,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if got := tc.pr.RestoredBytes(); got != tc.restored {
				t.Errorf("RestoredBytes() = %d, expected %d", got, tc.restored)
			}
		})
	}
}
