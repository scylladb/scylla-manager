// Copyright (C) 2017 ScyllaDB

package main

import (
	"encoding/json"
	"time"

	"github.com/scylladb/scylla-manager/pkg/service/healthcheck"
	"github.com/scylladb/scylla-manager/pkg/service/scheduler"
	"github.com/scylladb/scylla-manager/pkg/util/duration"
	"github.com/scylladb/scylla-manager/pkg/util/timeutc"
	"github.com/scylladb/scylla-manager/pkg/util/uuid"
)

func healthCheckModeProperties(mode healthcheck.Mode) json.RawMessage {
	return json.RawMessage(`{"mode": "` + mode.String() + `"}`)
}

func makeAutoHealthCheckTasks(clusterID uuid.UUID) []*scheduler.Task {
	return []*scheduler.Task{
		{
			ClusterID: clusterID,
			Type:      scheduler.HealthCheckTask,
			Enabled:   true,
			Name:      "cql",
			Sched: scheduler.Schedule{
				Interval:   duration.Duration(15 * time.Second),
				StartDate:  timeutc.Now().Add(30 * time.Second),
				NumRetries: 0,
			},
			Properties: healthCheckModeProperties(healthcheck.CQLMode),
		},
		{
			ClusterID: clusterID,
			Type:      scheduler.HealthCheckTask,
			Enabled:   true,
			Name:      "rest",
			Sched: scheduler.Schedule{
				Interval:   duration.Duration(1 * time.Minute),
				StartDate:  timeutc.Now().Add(2 * time.Minute),
				NumRetries: 0,
			},
			Properties: healthCheckModeProperties(healthcheck.RESTMode),
		},
		{
			ClusterID: clusterID,
			Type:      scheduler.HealthCheckTask,
			Enabled:   true,
			Name:      "alternator",
			Sched: scheduler.Schedule{
				Interval:   duration.Duration(15 * time.Second),
				StartDate:  timeutc.Now().Add(30 * time.Second),
				NumRetries: 0,
			},
			Properties: healthCheckModeProperties(healthcheck.AlternatorMode),
		},
	}
}

var emptyProperties = []byte{'{', '}'}

func makeAutoRepairTask(clusterID uuid.UUID) *scheduler.Task {
	return &scheduler.Task{
		ClusterID: clusterID,
		Type:      scheduler.RepairTask,
		Enabled:   true,
		Name:      "all-weekly",
		Sched: scheduler.Schedule{
			Interval:   duration.Duration(7 * 24 * time.Hour),
			StartDate:  timeutc.TodayMidnight(),
			NumRetries: 3,
		},
		Properties: emptyProperties,
	}
}
