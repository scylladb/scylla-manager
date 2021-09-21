// Copyright (C) 2017 ScyllaDB

package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/scylladb/scylla-manager/pkg/util/uuid"
)

type SchedulerMetrics struct {
	runIndicator *prometheus.GaugeVec
	runsTotal    *prometheus.GaugeVec
}

func NewSchedulerMetrics() SchedulerMetrics {
	g := gaugeVecCreator("scheduler")

	return SchedulerMetrics{
		runIndicator: g("If the task is running the value is 1 otherwise it's 0.",
			"run_indicator", "cluster", "type", "task"),
		runsTotal: g("Total number of task runs parametrized by status.",
			"run_total", "cluster", "type", "task", "status"),
	}
}

func (m SchedulerMetrics) all() []prometheus.Collector {
	return []prometheus.Collector{
		m.runIndicator,
		m.runsTotal,
	}
}

// MustRegister shall be called to make the metrics visible by prometheus client.
func (m SchedulerMetrics) MustRegister() SchedulerMetrics {
	prometheus.MustRegister(m.all()...)
	return m
}

// ResetClusterMetrics resets all metrics labeled with the cluster.
func (m SchedulerMetrics) ResetClusterMetrics(clusterID uuid.UUID) {
	for _, c := range m.all() {
		setGaugeVecMatching(c.(*prometheus.GaugeVec), unspecifiedValue, clusterMatcher(clusterID))
	}
}

// Init sets 0 values for all metrics.
func (m SchedulerMetrics) Init(clusterID uuid.UUID, taskType string, taskID uuid.UUID, statuses ...string) {
	m.runIndicator.WithLabelValues(clusterID.String(), taskType, taskID.String()).Add(0)
	for _, s := range statuses {
		m.runsTotal.WithLabelValues(clusterID.String(), taskType, taskID.String(), s).Add(0)
	}
}

// BeginRun updates "run_indicator".
func (m SchedulerMetrics) BeginRun(clusterID uuid.UUID, taskType string, taskID uuid.UUID) {
	m.runIndicator.WithLabelValues(clusterID.String(), taskType, taskID.String()).Inc()
}

// EndRun updates "run_indicator", and "runs_total".
func (m SchedulerMetrics) EndRun(clusterID uuid.UUID, taskType string, taskID uuid.UUID, status string) {
	m.runIndicator.WithLabelValues(clusterID.String(), taskType, taskID.String()).Dec()
	m.runsTotal.WithLabelValues(clusterID.String(), taskType, taskID.String(), status).Inc()
}
