// Copyright (C) 2017 ScyllaDB

package main

import (
	"time"

	"github.com/scylladb/mermaid/sched"
	"github.com/scylladb/mermaid/sched/runner"
	"github.com/scylladb/mermaid/uuid"
)

func midnight() time.Time {
	day := 24 * time.Hour
	return time.Now().Round(day).Add(day)
}

func repairAutoScheduleTask(clusterID uuid.UUID) *sched.Task {
	return &sched.Task{
		ClusterID: clusterID,
		Type:      sched.RepairAutoScheduleTask,
		Enabled:   true,
		Sched: sched.Schedule{
			Repeat:       true,
			IntervalDays: 7,
			StartDate:    midnight(),
			NumRetries:   sched.RetryFor(time.Hour),
		},
	}
}

func repairTask(clusterID uuid.UUID, props runner.TaskProperties) *sched.Task {
	return &sched.Task{
		ClusterID: clusterID,
		Type:      sched.RepairTask,
		Enabled:   true,
		Sched: sched.Schedule{
			Repeat:     false,
			StartDate:  midnight().Add(2 * time.Hour),
			NumRetries: sched.RetryFor(24 * 6 * time.Hour),
		},
		Properties: props,
	}
}
