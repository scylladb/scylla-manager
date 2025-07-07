// Copyright (C) 2017 ScyllaDB

package main

import (
	"encoding/json"
	"time"

	"github.com/scylladb/scylla-manager/v3/pkg/service/healthcheck"
	"github.com/scylladb/scylla-manager/v3/pkg/service/scheduler"
	"github.com/scylladb/scylla-manager/v3/pkg/util/schedules"
	"github.com/scylladb/scylla-manager/v3/pkg/util/timeutc"
	"github.com/scylladb/scylla-manager/v3/pkg/util/uuid"
)

func healthCheckModeProperties(mode healthcheck.Mode) json.RawMessage {
	return json.RawMessage(`{"mode": "` + mode.String() + `"}`)
}

func localTimezone() scheduler.Timezone {
	if timeutc.LocalName != "" {
		v, err := time.LoadLocation(timeutc.LocalName)
		if err == nil {
			return scheduler.NewTimezone(v)
		}
	}

	return scheduler.NewTimezone(nil)
}

func makeAutoHealthCheckTasks(clusterID uuid.UUID, cfg healthcheck.Config) []*scheduler.Task {
	return []*scheduler.Task{
		{
			ClusterID: clusterID,
			Type:      scheduler.HealthCheckTask,
			Enabled:   true,
			Name:      "cql",
			Sched: scheduler.Schedule{
				Cron:     cfg.CQLPingCron,
				Timezone: localTimezone(),
			},
			Properties: healthCheckModeProperties(healthcheck.CQLMode),
		},
		{
			ClusterID: clusterID,
			Type:      scheduler.HealthCheckTask,
			Enabled:   true,
			Name:      "rest",
			Sched: scheduler.Schedule{
				Cron:     cfg.RESTPingCron,
				Timezone: localTimezone(),
			},
			Properties: healthCheckModeProperties(healthcheck.RESTMode),
		},
		{
			ClusterID: clusterID,
			Type:      scheduler.HealthCheckTask,
			Enabled:   true,
			Name:      "alternator",
			Sched: scheduler.Schedule{
				Cron:     cfg.AlternatorPingCron,
				Timezone: localTimezone(),
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
			Cron:       schedules.MustCron("0 23 * * SAT", time.Time{}),
			Timezone:   localTimezone(),
			NumRetries: 3,
		},
		Properties: emptyProperties,
	}
}
