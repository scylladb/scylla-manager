// Copyright (C) 2026 ScyllaDB

package metrics

import "testing"

func TestTaskStateFromStatus(t *testing.T) {
	testCases := []struct {
		status string
		state  TaskState
		ok     bool
	}{
		{status: "RUNNING", state: TaskStateRunning, ok: true},
		{status: "STOPPING", state: TaskStateRunning, ok: true},
		{status: "DONE", state: TaskStateDone, ok: true},
		{status: "ERROR", state: TaskStateError, ok: true},
		{status: "STOPPED", state: TaskStateStopped, ok: true},
		{status: "ABORTED", state: TaskStateStopped, ok: true},
		{status: "NEW", state: TaskStateNew, ok: true},
		{status: "WAITING"},
		{status: ""},
	}

	for _, tc := range testCases {
		t.Run(tc.status, func(t *testing.T) {
			state, ok := taskStateFromStatus(tc.status)
			if ok != tc.ok {
				t.Fatalf("taskStateFromStatus(%q) ok = %v, expected %v", tc.status, ok, tc.ok)
			}
			if state != tc.state {
				t.Fatalf("taskStateFromStatus(%q) = %v, expected %v", tc.status, state, tc.state)
			}
		})
	}
}

func TestNormalizeTaskType(t *testing.T) {
	testCases := []struct {
		taskType string
		want     string
	}{
		{taskType: "tablet_repair", want: "repair"},
		{taskType: "repair", want: "repair"},
		{taskType: "backup", want: "backup"},
		{taskType: "restore", want: "restore"},
	}

	for _, tc := range testCases {
		t.Run(tc.taskType, func(t *testing.T) {
			if got := normalizeTaskType(tc.taskType); got != tc.want {
				t.Fatalf("normalizeTaskType(%q) = %q, expected %q", tc.taskType, got, tc.want)
			}
		})
	}
}
