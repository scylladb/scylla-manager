// Copyright (C) 2017 ScyllaDB

package main

import (
	"time"

	"github.com/scylladb/mermaid/sched"
	"github.com/scylladb/mermaid/sched/runner"
	"github.com/scylladb/mermaid/uuid"
)

const day = 24 * time.Hour

func midnight() time.Time {
	return time.Now().AddDate(0, 0, 1).Truncate(day).UTC()
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
			NumRetries: sched.RetryFor(6 * day),
		},
		Properties: props,
	}
}
