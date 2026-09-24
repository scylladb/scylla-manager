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

// Task statuses as defined by the scheduler service.
// They are duplicated here in order to avoid an import cycle
// (the scheduler service already depends on this package).
const (
	statusNew      = "NEW"
	statusRunning  = "RUNNING"
	statusStopping = "STOPPING"
	statusStopped  = "STOPPED"
	statusDone     = "DONE"
	statusError    = "ERROR"
	statusAborted  = "ABORTED"
)

// taskStateFromStatus maps scheduler task status to the state reported by
// the "scheduler_task_state" metric. The second return value is false for
// statuses that do not describe a task run (e.g. NEW or WAITING).
func taskStateFromStatus(status string) (TaskState, bool) {
	switch status {
	case statusNew:
		// The task is scheduled but has never run. Reported so that a task
		// which should have fired and did not is visible, instead of simply
		// missing from every panel driven by this metric.
		return TaskStateNew, true
	case statusRunning, statusStopping:
		return TaskStateRunning, true
	case statusDone:
		return TaskStateDone, true
	case statusError:
		return TaskStateError, true
	case statusStopped, statusAborted:
		return TaskStateStopped, true
	default:
		return 0, false
	}
}

// tabletRepairTaskType and repairTaskType are duplicated from the scheduler
// service for the same reason as the statuses above.
const (
	tabletRepairTaskType = "tablet_repair"
	repairTaskType       = "repair"
)

// normalizeTaskType maps the tablet repair task type to the general repair
// task type, so that filtering the "scheduler_task_state" metric by
// type="repair" returns both vnode and tablet repair tasks.
// Only the new per task metrics use it - the pre-existing scheduler metrics
// keep reporting the raw task type.
func normalizeTaskType(taskType string) string {
	if taskType == tabletRepairTaskType {
		return repairTaskType
	}
	return taskType
}
