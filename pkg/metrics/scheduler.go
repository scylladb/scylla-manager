// Copyright (C) 2017 ScyllaDB

package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/scylladb/scylla-manager/v3/pkg/util/uuid"
)

type SchedulerMetrics struct {
	suspended    *prometheus.GaugeVec
	runIndicator *prometheus.GaugeVec
	runsTotal    *prometheus.GaugeVec
	lastSuccess  *prometheus.GaugeVec

	taskState *prometheus.GaugeVec
}

func NewSchedulerMetrics() SchedulerMetrics {
	g := gaugeVecCreator("scheduler")

	return SchedulerMetrics{
		suspended: g("If the cluster is suspended the value is 1 otherwise it's 0.",
			"suspended", "cluster"),
		runIndicator: g("If the task is running the value is 1 otherwise it's 0.",
			"run_indicator", "cluster", "type", "task"),
		runsTotal: g("Total number of task runs parametrized by status.",
			"run_total", "cluster", "type", "task", "status"),
		lastSuccess: g("Start time of the last successful run as a Unix timestamp.",
			"last_success", "cluster", "type", "task"),
		taskState: g("State of the task, as \"sctool tasks\" reports it: "+
			"0 - never run, 1 - currently running, 2 - last run is done, "+
			"3 - last run ended in error, 4 - last run was stopped. "+
			"A run aborted by a Scylla Manager restart, or cut short by the end "+
			"of its maintenance window, reads as stopped.",
			"task_state", "cluster", "type", "task"),
	}
}

func (m SchedulerMetrics) all() []prometheus.Collector {
	return []prometheus.Collector{
		m.suspended,
		m.runIndicator,
		m.runsTotal,
		m.lastSuccess,
		m.taskState,
	}
}

// MustRegister shall be called to make the metrics visible by prometheus client.
func (m SchedulerMetrics) MustRegister() SchedulerMetrics {
	return m.MustRegisterWith(prometheus.DefaultRegisterer)
}

// MustRegisterWith registers all scheduler metrics with the given registerer.
func (m SchedulerMetrics) MustRegisterWith(reg prometheus.Registerer) SchedulerMetrics {
	reg.MustRegister(m.all()...)
	return m
}

// ResetClusterMetrics resets all metrics labeled with the cluster.
func (m SchedulerMetrics) ResetClusterMetrics(clusterID uuid.UUID) {
	for _, c := range m.all() {
		setGaugeVecMatching(c.(*prometheus.GaugeVec), unspecifiedValue, clusterMatcher(clusterID))
	}
}

// DeleteTaskMetrics removes all metrics labeled with the task.
func (m SchedulerMetrics) DeleteTaskMetrics(taskID uuid.UUID) {
	l := prometheus.Labels{"task": taskID.String()}
	for _, c := range []*prometheus.GaugeVec{
		m.runIndicator, m.runsTotal, m.lastSuccess, m.taskState,
	} {
		c.DeletePartialMatch(l)
	}
}

// Init sets 0 values for all metrics.
func (m SchedulerMetrics) Init(clusterID uuid.UUID, taskType string, taskID uuid.UUID, statuses ...string) {
	m.runIndicator.WithLabelValues(clusterID.String(), taskType, taskID.String()).Add(0)
	for _, s := range statuses {
		m.runsTotal.WithLabelValues(clusterID.String(), taskType, taskID.String(), s).Add(0)
	}
}

// SetTaskState sets "task_state" to the given state.
func (m SchedulerMetrics) SetTaskState(clusterID uuid.UUID, taskType string, taskID uuid.UUID, state TaskState) {
	m.taskState.WithLabelValues(clusterID.String(), taskType, taskID.String()).Set(float64(state))
}

// BeginRun updates "run_indicator" and "task_state".
func (m SchedulerMetrics) BeginRun(clusterID uuid.UUID, taskType string, taskID uuid.UUID) {
	m.runIndicator.WithLabelValues(clusterID.String(), taskType, taskID.String()).Inc()
	m.SetTaskState(clusterID, taskType, taskID, TaskStateRunning)
}

// EndRun updates "run_indicator", "runs_total", and "last_success".
func (m SchedulerMetrics) EndRun(clusterID uuid.UUID, taskType string, taskID uuid.UUID, status string, startTime int64) {
	m.runIndicator.WithLabelValues(clusterID.String(), taskType, taskID.String()).Dec()
	m.runsTotal.WithLabelValues(clusterID.String(), taskType, taskID.String(), status).Inc()
	if status == "DONE" {
		m.lastSuccess.WithLabelValues(clusterID.String(), taskType, taskID.String()).Set(float64(startTime))
	}
}

// Suspend sets "suspend" to 1.
func (m SchedulerMetrics) Suspend(clusterID uuid.UUID) {
	m.suspended.WithLabelValues(clusterID.String()).Set(1)
}

// Resume sets "suspend" to 0.
func (m SchedulerMetrics) Resume(clusterID uuid.UUID) {
	m.suspended.WithLabelValues(clusterID.String()).Set(0)
}
