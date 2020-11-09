// Copyright (C) 2017 ScyllaDB

package main

import (
	"time"

	"github.com/scylladb/scylla-manager/pkg/service/scheduler"
	"github.com/scylladb/scylla-manager/pkg/util/duration"
	"github.com/scylladb/scylla-manager/pkg/util/timeutc"
	"github.com/scylladb/scylla-manager/pkg/util/uuid"
)

var emptyProperties = []byte{'{', '}'}

func makeAutoHealthCheckTask(clusterID uuid.UUID) *scheduler.Task {
	return &scheduler.Task{
		ClusterID: clusterID,
		Type:      scheduler.HealthCheckCQLTask,
		Enabled:   true,
		Sched: scheduler.Schedule{
			Interval:   duration.Duration(15 * time.Second),
			StartDate:  timeutc.Now().Add(30 * time.Second),
			NumRetries: 0,
		},
		Properties: emptyProperties,
	}
}

func makeAutoHealthCheckAlternatorTask(clusterID uuid.UUID) *scheduler.Task {
	return &scheduler.Task{
		ClusterID: clusterID,
		Type:      scheduler.HealthCheckAlternatorTask,
		Enabled:   true,
		Sched: scheduler.Schedule{
			Interval:   duration.Duration(15 * time.Second),
			StartDate:  timeutc.Now().Add(30 * time.Second),
			NumRetries: 0,
		},
		Properties: emptyProperties,
	}
}

func makeAutoHealthCheckRESTTask(clusterID uuid.UUID) *scheduler.Task {
	return &scheduler.Task{
		ClusterID: clusterID,
		Type:      scheduler.HealthCheckRESTTask,
		Enabled:   true,
		Sched: scheduler.Schedule{
			Interval:   duration.Duration(1 * time.Minute),
			StartDate:  timeutc.Now().Add(2 * time.Minute),
			NumRetries: 0,
		},
		Properties: emptyProperties,
	}
}

func makeAutoRepairTask(clusterID uuid.UUID) *scheduler.Task {
	return &scheduler.Task{
		ClusterID: clusterID,
		Type:      scheduler.RepairTask,
		Enabled:   true,
		Sched: scheduler.Schedule{
			Interval:   duration.Duration(7 * 24 * time.Hour),
			StartDate:  timeutc.TodayMidnight(),
			NumRetries: 3,
		},
		Properties: emptyProperties,
	}
}
