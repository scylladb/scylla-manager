// Copyright (C) 2017 ScyllaDB

package scheduler

import (
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

var (
	taskActiveCount = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: "scylla_manager",
		Subsystem: "task",
		Name:      "active_count",
		Help:      "Total number of active (in-flight) tasks.",
	}, []string{"cluster", "type", "task"})

	taskRunTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: "scylla_manager",
		Subsystem: "task",
		Name:      "run_total",
		Help:      "Total number of task runs.",
	}, []string{"cluster", "type", "task", "status"})

	taskLastSuccess = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: "scylla_manager",
		Subsystem: "task",
		Name:      "last_success",
		Help:      "Start time of the last successful run as a Unix timestamp.",
	}, []string{"cluster", "type", "task"})

	taskLastRunDurationSeconds = prometheus.NewSummaryVec(prometheus.SummaryOpts{
		Namespace: "scylla_manager",
		Subsystem: "task",
		Name:      "last_run_duration_seconds",
		Help:      "Duration of a last run of a task.",
		MaxAge:    30 * time.Minute,
	}, []string{"cluster", "task", "type", "status"})
)

func init() {
	prometheus.MustRegister(
		taskActiveCount,
		taskRunTotal,
		taskLastSuccess,
		taskLastRunDurationSeconds,
	)
}
