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
			name: "backup task falls back to the defaults",
			task: Task{Name: "defaults", Type: BackupTask},
			golden: metrics.TaskInfo{
				Name:              "defaults",
				Cron:              metrics.TaskScheduleAdHoc,
				Keyspace:          "all",
				DC:                "all",
				Retention:         "3",
				RetentionDays:     "0",
				Method:            "rclone",
				PurgeOnly:         "false",
				SkipSchema:        "false",
				RetentionLockMode: "disabled",
			},
		},
		{
			name: "configured backup properties win over the defaults",
			task: Task{
				Name: "weekly",
				Type: BackupTask,
				Properties: json.RawMessage(`{
					"keyspace": ["ks", "!ks.tbl"],
					"dc": ["dc1"],
					"location": ["dc1:s3:backups", "s3:other"],
					"retention_days": 30,
					"method": "native",
					"skip_schema": true,
					"retention_lock_mode": "locked",
					"transfers": 4
				}`),
			},
			golden: metrics.TaskInfo{
				Name:     "weekly",
				Cron:     metrics.TaskScheduleAdHoc,
				Keyspace: "ks,!ks.tbl",
				DC:       "dc1",
				Location: "dc1:s3:backups,s3:other",
				// Configuring only retention_days leaves retention at 0 - the
				// fallback to 3 applies only when neither is set.
				Retention:         "0",
				RetentionDays:     "30",
				Method:            "native",
				PurgeOnly:         "false",
				SkipSchema:        "true",
				RetentionLockMode: "locked",
			},
		},
		{
			name: "backup defaults are not applied to a repair task",
			task: Task{
				Name:       "r",
				Type:       RepairTask,
				Properties: json.RawMessage(`{"keyspace":["ks"]}`),
			},
			golden: metrics.TaskInfo{
				Name:                "r",
				Cron:                metrics.TaskScheduleAdHoc,
				Keyspace:            "ks",
				KeyspaceReplication: "all",
				IncrementalMode:     "incremental",
				DC:                  "all",
				Host:                "all",
				FailFast:            "false",
			},
		},
		{
			name:   "defaults are not applied to other task types",
			task:   Task{Name: "v", Type: ValidateBackupTask},
			golden: metrics.TaskInfo{Name: "v", Cron: metrics.TaskScheduleAdHoc},
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
