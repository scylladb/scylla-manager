// Copyright (C) 2017 ScyllaDB

package main

import (
	"time"

	"github.com/scylladb/mermaid/internal/timeutc"
	"github.com/scylladb/mermaid/sched"
	"github.com/scylladb/mermaid/uuid"
)

func makeAutoHealthCheckTask(clusterID uuid.UUID) *sched.Task {
	return &sched.Task{
		ClusterID: clusterID,
		Type:      sched.HealthCheckTask,
		Enabled:   true,
		Sched: sched.Schedule{
			Interval:   sched.Duration(15 * time.Second),
			StartDate:  timeutc.Now().Add(30 * time.Second),
			NumRetries: 0,
		},
		Properties: []byte{'{', '}'},
	}
}

func makeAutoRepairTask(clusterID uuid.UUID) *sched.Task {
	return &sched.Task{
		ClusterID: clusterID,
		Type:      sched.RepairTask,
		Enabled:   true,
		Sched: sched.Schedule{
			Interval:   sched.Duration(7 * 24 * time.Hour),
			StartDate:  timeutc.TodayMidnight(),
			NumRetries: 3,
		},
		Properties: []byte{'{', '}'},
	}
}
