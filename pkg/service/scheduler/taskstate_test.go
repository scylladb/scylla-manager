// Copyright (C) 2026 ScyllaDB

package scheduler

import (
	"testing"

	"github.com/scylladb/scylla-manager/v3/pkg/metrics"
)

func TestTaskStateFromStatus(t *testing.T) {
	testCases := []struct {
		status Status
		want   metrics.TaskState
		ok     bool
	}{
		{status: StatusNew, want: metrics.TaskStateNew, ok: true},
		{status: StatusRunning, want: metrics.TaskStateRunning, ok: true},
		{status: StatusStopping, want: metrics.TaskStateRunning, ok: true},
		{status: StatusDone, want: metrics.TaskStateDone, ok: true},
		{status: StatusError, want: metrics.TaskStateError, ok: true},
		{status: StatusStopped, want: metrics.TaskStateStopped, ok: true},
		// An SM crash leaves the task RUNNING, and markRunningAsAborted turns
		// it into ABORTED before the metrics are initialized, so the aborted
		// run is reported like any other stopped one.
		{status: StatusAborted, want: metrics.TaskStateStopped, ok: true},
		// A run cut short by the end of its maintenance window ends in
		// WAITING, so it is reported like any other run that did not finish.
		{status: StatusWaiting, want: metrics.TaskStateStopped, ok: true},
	}

	for _, tc := range testCases {
		t.Run(string(tc.status), func(t *testing.T) {
			got, ok := taskStateFromStatus(tc.status)
			if ok != tc.ok {
				t.Fatalf("ok = %v, expected %v", ok, tc.ok)
			}
			if ok && got != tc.want {
				t.Errorf("state = %v, expected %v", got, tc.want)
			}
		})
	}
}
