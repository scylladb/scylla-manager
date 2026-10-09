// Copyright (C) 2026 ScyllaDB

package scheduler

import "github.com/scylladb/scylla-manager/v3/pkg/metrics"

// taskSchedule describes when a task is expected to run. It reports the cron
// specification, falling back to the deprecated interval, and names a task
// that is not scheduled at all.
func taskSchedule(t *Task) string {
	if !t.Sched.Cron.IsZero() {
		return t.Sched.Cron.Spec
	}
	if t.Sched.Interval != 0 {
		return t.Sched.Interval.String()
	}
	// Named rather than left empty, both because "not scheduled" is a
	// property of the task and not a missing value, and because it sorts
	// after any cron specification, which keeps ad-hoc tasks at the end of
	// a list ordered by schedule.
	return metrics.TaskScheduleAdHoc
}
