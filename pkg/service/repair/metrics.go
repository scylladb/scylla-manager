// Copyright (C) 2017 ScyllaDB

package repair

import (
	"github.com/prometheus/client_golang/prometheus"
)

var (
	repairSegmentsTotal = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: "scylla_manager",
		Subsystem: "repair",
		Name:      "segments_total",
		Help:      "Total number of segments to repair.",
	}, []string{"cluster", "task", "keyspace", "host"})

	repairSegmentsSuccess = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: "scylla_manager",
		Subsystem: "repair",
		Name:      "segments_success",
		Help:      "Number of repaired segments.",
	}, []string{"cluster", "task", "keyspace", "host"})

	repairSegmentsError = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: "scylla_manager",
		Subsystem: "repair",
		Name:      "segments_error",
		Help:      "Number of segments that failed to repair.",
	}, []string{"cluster", "task", "keyspace", "host"})

	repairProgress = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: "scylla_manager",
		Subsystem: "repair",
		Name:      "progress",
		Help:      "Current repair progress.",
	}, []string{"cluster", "task", "keyspace", "host"})

	repairInflightJobs = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: "scylla_manager",
		Subsystem: "repair",
		Name:      "inflight_jobs",
		Help:      "Number of jobs that are currently being processed",
	}, []string{"cluster", "task", "host"})
)

func init() {
	prometheus.MustRegister(
		repairSegmentsTotal,
		repairSegmentsSuccess,
		repairSegmentsError,
		repairProgress,
		repairInflightJobs,
	)
}

func updateMetrics(run *Run, prog Progress) {
	taskID := run.TaskID.String()

	for _, h := range prog.Hosts {
		for k, v := range keyspaceProgress(h.Tables) {
			repairProgress.With(prometheus.Labels{
				"cluster":  run.clusterName,
				"task":     taskID,
				"keyspace": k,
				"host":     h.Host,
			}).Set(float64(v))
		}
	}

	// Aggregated keyspace progress
	for k, v := range keyspaceProgress(prog.Tables) {
		repairProgress.With(prometheus.Labels{
			"cluster":  run.clusterName,
			"task":     taskID,
			"keyspace": k,
			"host":     "",
		}).Set(float64(v))
	}

	// Aggregated total progress
	repairProgress.With(prometheus.Labels{
		"cluster":  run.clusterName,
		"task":     taskID,
		"keyspace": "",
		"host":     "",
	}).Set(float64(prog.PercentComplete()))
}

func keyspaceProgress(tables []TableProgress) map[string]int {
	keyspaceProgress := make(map[string]int)
	for _, t := range tables {
		keyspaceProgress[t.Keyspace] = (keyspaceProgress[t.Keyspace] + t.PercentComplete()) / 2
	}
	return keyspaceProgress
}
