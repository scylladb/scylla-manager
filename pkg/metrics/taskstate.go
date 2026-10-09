// Copyright (C) 2026 ScyllaDB

package metrics

// TaskState describes the state of a task run reported
// by the "scheduler_task_state" metric.
// It is encoded as a metric value (and not as a label), so that a single
// series per task can be rendered as a state timeline.
type TaskState float64

// Task states reported by the "scheduler_task_state" metric.
const (
	// TaskStateNew means that the task exists but has not run yet.
	TaskStateNew TaskState = 0
	// TaskStateRunning means that the task run is in progress.
	TaskStateRunning TaskState = 1
	// TaskStateDone means that the last task run finished successfully.
	TaskStateDone TaskState = 2
	// TaskStateError means that the last task run finished with an error.
	TaskStateError TaskState = 3
	// TaskStateStopped means that the last task run was stopped or aborted.
	TaskStateStopped TaskState = 4
)
