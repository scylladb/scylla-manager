// Copyright (C) 2017 ScyllaDB

package scheduler

import (
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

var (
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
		taskLastRunDurationSeconds,
	)
}
