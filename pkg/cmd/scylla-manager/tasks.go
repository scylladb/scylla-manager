// Copyright (C) 2017 ScyllaDB

package main

import (
	"encoding/json"
	"time"

	"github.com/scylladb/scylla-manager/pkg/service/healthcheck"
	"github.com/scylladb/scylla-manager/pkg/service/scheduler"
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
				Cron: scheduler.NewCronEvery(15 * time.Second),
			},
			Properties: healthCheckModeProperties(healthcheck.CQLMode),
		},
		{
			ClusterID: clusterID,
			Type:      scheduler.HealthCheckTask,
			Enabled:   true,
			Name:      "rest",
			Sched: scheduler.Schedule{
				Cron: scheduler.NewCronEvery(1 * time.Minute),
			},
			Properties: healthCheckModeProperties(healthcheck.RESTMode),
		},
		{
			ClusterID: clusterID,
			Type:      scheduler.HealthCheckTask,
			Enabled:   true,
			Name:      "alternator",
			Sched: scheduler.Schedule{
				Cron: scheduler.NewCronEvery(15 * time.Second),
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
			Cron:       scheduler.MustCron("0 23 * * SAT"),
			NumRetries: 3,
		},
		Properties: emptyProperties,
	}
}
