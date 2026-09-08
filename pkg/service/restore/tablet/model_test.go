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
