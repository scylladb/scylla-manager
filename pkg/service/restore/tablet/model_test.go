// Copyright (C) 2026 ScyllaDB

package tablet

import (
	"testing"
	"time"

	"github.com/scylladb/scylla-manager/v3/pkg/util/timeutc"
)

func TestRunProgressPredicates(t *testing.T) {
	now := timeutc.Now()

	testCases := []struct {
		name        string
		pr          RunProgress
		isSuccess   bool
		canReattach bool
	}{
		{
			name:        "zero value",
			pr:          RunProgress{},
			isSuccess:   false,
			canReattach: false,
		},
		{
			name:        "started, task not scheduled yet",
			pr:          RunProgress{StartedAt: now},
			isSuccess:   false,
			canReattach: false,
		},
		{
			name:        "task scheduled, outcome unknown (SM crash)",
			pr:          RunProgress{StartedAt: now, Host: "h", ScyllaTaskID: "task"},
			isSuccess:   false,
			canReattach: true,
		},
		{
			name:        "task ID without host",
			pr:          RunProgress{StartedAt: now, ScyllaTaskID: "task"},
			isSuccess:   false,
			canReattach: false,
		},
		{
			name:        "completed without error",
			pr:          RunProgress{StartedAt: now, Host: "h", ScyllaTaskID: "task", CompletedAt: now.Add(time.Minute)},
			isSuccess:   true,
			canReattach: false,
		},
		{
			name:        "completed with error",
			pr:          RunProgress{StartedAt: now, Host: "h", ScyllaTaskID: "task", CompletedAt: now.Add(time.Minute), Error: "task failed"},
			isSuccess:   false,
			canReattach: false,
		},
		{
			name:        "error recorded without completion timestamp",
			pr:          RunProgress{StartedAt: now, Host: "h", ScyllaTaskID: "task", Error: "ctx cancelled"},
			isSuccess:   false,
			canReattach: false,
		},
		{
			name:        "error recorded before scheduling the task",
			pr:          RunProgress{StartedAt: now, Error: "schedule failed"},
			isSuccess:   false,
			canReattach: false,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			if got := tc.pr.isSuccess(); got != tc.isSuccess {
				t.Errorf("isSuccess() = %v, expected %v", got, tc.isSuccess)
			}
			if got := tc.pr.canReattach(); got != tc.canReattach {
				t.Errorf("canReattach() = %v, expected %v", got, tc.canReattach)
			}
		})
	}
}
