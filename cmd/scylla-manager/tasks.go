// Copyright (C) 2017 ScyllaDB

package main

import (
	"time"

	"github.com/scylladb/mermaid/sched"
	"github.com/scylladb/mermaid/sched/runner"
	"github.com/scylladb/mermaid/uuid"
)

func repairAutoScheduleTask(clusterID uuid.UUID) *sched.Task {
	return &sched.Task{
		ClusterID: clusterID,
		Type:      sched.RepairAutoScheduleTask,
		Enabled:   true,
		Sched: sched.Schedule{
			IntervalDays: 7,
			StartDate:    midnight(),
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
			StartDate:  nowPlus(timeMargin),
			NumRetries: sched.RetryFor(6 * day),
		},
		Properties: props,
	}
}
