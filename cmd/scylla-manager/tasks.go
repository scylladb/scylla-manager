// Copyright (C) 2017 ScyllaDB

package main

import (
	"time"

	"github.com/scylladb/mermaid/sched"
	"github.com/scylladb/mermaid/sched/runner"
	"github.com/scylladb/mermaid/timeutc"
	"github.com/scylladb/mermaid/uuid"
)

func repairAutoScheduleTask(clusterID uuid.UUID) *sched.Task {
	return &sched.Task{
		ClusterID: clusterID,
		Type:      sched.RepairAutoScheduleTask,
		Enabled:   true,
		Sched: sched.Schedule{
			IntervalDays: 7,
			StartDate:    timeutc.TodayMidnight(),
			NumRetries:   sched.RetryFor(time.Hour),
		},
	}
}

func repairTask(clusterID uuid.UUID, props runner.TaskProperties, timeMargin time.Duration) *sched.Task {
	return &sched.Task{
		ClusterID: clusterID,
		Type:      sched.RepairTask,
		Enabled:   true,
		Sched: sched.Schedule{
			StartDate:  timeutc.Now().Add(timeMargin).Truncate(time.Minute),
			NumRetries: sched.RetryFor(6 * 24 * time.Hour),
		},
		Properties: props,
	}
}
