// Copyright (C) 2026 ScyllaDB

package scheduler

import (
	"testing"
	"time"

	"github.com/scylladb/scylla-manager/v3/pkg/metrics"
	"github.com/scylladb/scylla-manager/v3/pkg/util/duration"
	"github.com/scylladb/scylla-manager/v3/pkg/util/schedules"
)

func TestTaskSchedule(t *testing.T) {
	testCases := []struct {
		name string
		task Task
		want string
	}{
		{
			name: "cron",
			task: Task{Sched: Schedule{Cron: schedules.MustCron("0 23 * * SAT", time.Time{})}},
			want: "0 23 * * SAT",
		},
		{
			name: "falls back to the deprecated interval",
			task: Task{Sched: Schedule{Interval: duration.Duration(7 * 24 * time.Hour)}},
			want: "7d",
		},
		{
			name: "not scheduled at all",
			task: Task{},
			want: metrics.TaskScheduleAdHoc,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			if got := taskSchedule(&tc.task); got != tc.want {
				t.Errorf("taskSchedule() = %q, expected %q", got, tc.want)
			}
		})
	}
}
