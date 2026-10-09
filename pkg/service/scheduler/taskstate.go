// Copyright (C) 2026 ScyllaDB

package scheduler

import "github.com/scylladb/scylla-manager/v3/pkg/metrics"

// taskStateFromStatus maps a task status to the state reported by the
// "scheduler_task_state" metric. The second return value is false for
// statuses that do not describe a task run, which are not reported at all.
//
// The mapping lives here rather than in pkg/metrics, so that the statuses
// do not have to be duplicated there - the scheduler owns them.
func taskStateFromStatus(status Status) (metrics.TaskState, bool) {
	switch status {
	case StatusNew:
		// The task is scheduled but has never run. Reported so that a task
		// which should have fired and did not is visible, instead of simply
		// missing from every panel driven by this metric.
		return metrics.TaskStateNew, true
	case StatusRunning, StatusStopping:
		return metrics.TaskStateRunning, true
	case StatusDone:
		return metrics.TaskStateDone, true
	case StatusError:
		return metrics.TaskStateError, true
	case StatusStopped, StatusAborted, StatusWaiting:
		// WAITING is a terminal status of a run that was cut short by the end
		// of its maintenance window, so it is reported like any other run that
		// did not finish. Leaving it out would latch the task at "running"
		// until it ran again.
		return metrics.TaskStateStopped, true
	default:
		return 0, false
	}
}
