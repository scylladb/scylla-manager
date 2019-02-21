// Copyright (C) 2017 ScyllaDB

package main

import (
	"time"

	"github.com/scylladb/mermaid/internal/duration"
	"github.com/scylladb/mermaid/internal/timeutc"
	"github.com/scylladb/mermaid/sched"
	"github.com/scylladb/mermaid/uuid"
)

var emptyProperties = []byte{'{', '}'}

func makeAutoHealthCheckTask(clusterID uuid.UUID) *sched.Task {
	return &sched.Task{
		ClusterID: clusterID,
		Type:      sched.HealthCheckTask,
		Enabled:   true,
		Sched: sched.Schedule{
			Interval:   duration.Duration(15 * time.Second),
			StartDate:  timeutc.Now().Add(30 * time.Second),
			NumRetries: 0,
		},
		Properties: emptyProperties,
	}
}

func makeAutoHealthCheckAPITask(clusterID uuid.UUID) *sched.Task {
	return &sched.Task{
		ClusterID: clusterID,
		Type:      sched.HealthCheckAPITask,
		Enabled:   true,
		Sched: sched.Schedule{
			Interval:   duration.Duration(1 * time.Hour),
			StartDate:  timeutc.Now().Add(1 * time.Minute),
			NumRetries: 0,
		},
		Properties: emptyProperties,
	}
}

func makeAutoRepairTask(clusterID uuid.UUID) *sched.Task {
	return &sched.Task{
		ClusterID: clusterID,
		Type:      sched.RepairTask,
		Enabled:   true,
		Sched: sched.Schedule{
			Interval:   duration.Duration(7 * 24 * time.Hour),
			StartDate:  timeutc.TodayMidnight(),
			NumRetries: 3,
		},
		Properties: emptyProperties,
	}
}
