// Copyright (C) 2026 ScyllaDB

package scheduler

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/scylladb/scylla-manager/v3/pkg/util/duration"
	"github.com/scylladb/scylla-manager/v3/pkg/util/schedules"

	"github.com/google/go-cmp/cmp"
	"github.com/scylladb/scylla-manager/v3/pkg/metrics"
)

func TestNewTaskInfo(t *testing.T) {
	testCases := []struct {
		name   string
		task   Task
		golden metrics.TaskInfo
	}{
		{
			name:   "no properties",
			task:   Task{Name: "every-5min"},
			golden: metrics.TaskInfo{Name: "every-5min", Cron: metrics.TaskScheduleAdHoc},
		},
		{
			name:   "cron schedule",
			task:   Task{Name: "weekly", Sched: Schedule{Cron: schedules.MustCron("0 23 * * SAT", time.Time{})}},
			golden: metrics.TaskInfo{Name: "weekly", Cron: "0 23 * * SAT"},
		},
		{
			name:   "falls back to the deprecated interval",
			task:   Task{Sched: Schedule{Interval: duration.Duration(7 * 24 * time.Hour)}},
			golden: metrics.TaskInfo{Cron: "7d"},
		},
		{
			name:   "no schedule at all",
			task:   Task{Name: "one-off"},
			golden: metrics.TaskInfo{Name: "one-off", Cron: metrics.TaskScheduleAdHoc},
		},
		{
			name:   "empty properties",
			task:   Task{Name: "t", Properties: json.RawMessage(`{}`)},
			golden: metrics.TaskInfo{Name: "t", Cron: metrics.TaskScheduleAdHoc},
		},
		{
			name:   "only configured properties are reported",
			task:   Task{Properties: json.RawMessage(`{"intensity":2,"parallel":3}`)},
			golden: metrics.TaskInfo{Cron: metrics.TaskScheduleAdHoc},
		},
		{
			name: "repair task falls back to the defaults",
			task: Task{Name: "defaults", Type: RepairTask},
			golden: metrics.TaskInfo{
				Name:                "defaults",
				Cron:                metrics.TaskScheduleAdHoc,
				Keyspace:            "*,!system_traces",
				KeyspaceReplication: "all",
				IncrementalMode:     "incremental",
				DC:                  "all",
				Host:                "all",
				FailFast:            "false",
			},
		},
		{
			name: "configured repair properties win over the defaults",
			task: Task{
				Type:       RepairTask,
				Properties: json.RawMessage(`{"keyspace":["ks"],"dc":["dc1"],"fail_fast":true}`),
			},
			golden: metrics.TaskInfo{
				Cron:                metrics.TaskScheduleAdHoc,
				Keyspace:            "ks",
				KeyspaceReplication: "all",
				IncrementalMode:     "incremental",
				DC:                  "dc1",
				Host:                "all",
				FailFast:            "true",
			},
		},
		{
			name:   "defaults are not applied to other task types",
			task:   Task{Name: "b", Type: BackupTask},
			golden: metrics.TaskInfo{Name: "b", Cron: metrics.TaskScheduleAdHoc},
		},
		{
			name: "full repair properties",
			task: Task{
				Name: "weekly",
				Properties: json.RawMessage(`{
					"keyspace": ["*", "!system_traces"],
					"keyspace_replication": "tablet",
					"incremental_mode": "full",
					"dc": ["dc1", "dc2"],
					"host": "192.168.200.11",
					"fail_fast": true,
					"intensity": 2
				}`),
			},
			golden: metrics.TaskInfo{
				Name:                "weekly",
				Cron:                metrics.TaskScheduleAdHoc,
				Keyspace:            "*,!system_traces",
				KeyspaceReplication: "tablet",
				IncrementalMode:     "full",
				DC:                  "dc1,dc2",
				Host:                "192.168.200.11",
				FailFast:            "true",
			},
		},
		{
			name:   "fail_fast false is reported, unset is not",
			task:   Task{Properties: json.RawMessage(`{"fail_fast": false}`)},
			golden: metrics.TaskInfo{Cron: metrics.TaskScheduleAdHoc, FailFast: "false"},
		},
		{
			name:   "unreadable properties still describe the task",
			task:   Task{Name: "broken", Properties: json.RawMessage(`not json`)},
			golden: metrics.TaskInfo{Name: "broken", Cron: metrics.TaskScheduleAdHoc},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			if diff := cmp.Diff(newTaskInfo(&tc.task), tc.golden); diff != "" {
				t.Error(diff)
			}
		})
	}
}
