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

	taskState           *prometheus.GaugeVec
	taskRunStartSeconds *prometheus.GaugeVec
	taskProperties      *prometheus.GaugeVec
}

// PropertyUnset is reported for a property that was not configured and has
// no Scylla Manager side default.
// An empty label value cannot be used, because Prometheus treats an empty
// label as an absent one, which would drop the property from the series
// entirely and make it impossible to render as a fixed table column.
const PropertyUnset = "-"

// TaskScheduleAdHoc is reported in the "cron" label of a task that is not
// scheduled at all. It sorts after any cron specification, so a list
// ordered by schedule keeps ad-hoc tasks at the end.
const TaskScheduleAdHoc = "ad-hoc"

func NewSchedulerMetrics() SchedulerMetrics {
	g := gaugeVecCreator("scheduler")

	return SchedulerMetrics{
		suspended: g("If the cluster is suspended the value is 1 otherwise it's 0.",
			"suspended", "cluster"),
		runIndicator: g("If the task is running the value is 1 otherwise it's 0.",
			"run_indicator", "cluster", "type", "task"),
		runsTotal: g("Total number of task runs parametrized by status.",
			"run_total", "cluster", "type", "task", "status"),
		lastSuccess: g("Start time of the last successful run as a Unix timestamp. "+
			"The start time is reported, and not the end time, because it describes "+
			"the state of the data the run worked on - a repair makes the data as "+
			"fresh as of the moment it started, and a backup stores the snapshot "+
			"taken at that moment.",
			"last_success", "cluster", "type", "task"),
		taskState: g("State of the task, as \"sctool tasks\" reports it: "+
			"0 - never run, 1 - currently running, 2 - last run is done, "+
			"3 - last run ended in error, 4 - last run was stopped. "+
			"A run aborted by a Scylla Manager restart, or cut short by the end "+
			"of its maintenance window, reads as stopped.",
			"task_state", "cluster", "type", "task"),
		taskRunStartSeconds: g("Start time of the last task run as a Unix timestamp. "+
			"Together with \"task_state\" it tells for how long a task has been running.",
			"task_run_start_seconds", "cluster", "type", "task"),
		taskProperties: g("Scheduler properties of a task, always 1. "+
			"Join it to other task metrics on the \"task\" label, e.g. "+
			"\"task_state * on(task) group_left(name) scheduler_task_properties\". "+
			"The properties the task type owns are reported by that type - see "+
			"\"scylla_manager_repair_task_properties\" and "+
			"\"scylla_manager_backup_task_properties\". "+
			"The \"name\" label falls back to the task ID for unnamed tasks, so "+
			"that two unnamed tasks stay apart when a panel groups by name.",
			"task_properties", "cluster", "type", "task", "name", "cron"),
	}
}

func (m SchedulerMetrics) all() []prometheus.Collector {
	return []prometheus.Collector{
		m.suspended,
		m.runIndicator,
		m.runsTotal,
		m.lastSuccess,
		m.taskState,
		m.taskRunStartSeconds,
		m.taskProperties,
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
		m.runIndicator, m.runsTotal, m.lastSuccess,
		m.taskState, m.taskRunStartSeconds, m.taskProperties,
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

// SetLastSuccess sets "last_success" to the start of the given run.
func (m SchedulerMetrics) SetLastSuccess(clusterID uuid.UUID, taskType string, taskID uuid.UUID, startTime int64) {
	m.lastSuccess.WithLabelValues(clusterID.String(), taskType, taskID.String()).Set(float64(startTime))
}

// SetTaskRunStart sets "task_run_start_seconds" to the start of the given run.
func (m SchedulerMetrics) SetTaskRunStart(clusterID uuid.UUID, taskType string, taskID uuid.UUID, startTime int64) {
	m.taskRunStartSeconds.WithLabelValues(clusterID.String(), taskType, taskID.String()).Set(float64(startTime))
}

// SetTaskProperties updates "task_properties" with the scheduler properties
// of the task. The previous series of the task is removed first, so that
// renaming or rescheduling replaces it instead of leaving a stale one behind.
func (m SchedulerMetrics) SetTaskProperties(clusterID uuid.UUID, taskType string, taskID uuid.UUID, name, cron string) {
	m.taskProperties.DeletePartialMatch(prometheus.Labels{"task": taskID.String()})
	m.taskProperties.WithLabelValues(
		clusterID.String(), taskType, taskID.String(), taskName(name, taskID), orUnset(cron),
	).Set(1)
}

// taskName falls back to the task ID for unnamed tasks. A task does not have
// to be named - an ad-hoc one usually isn't - and sctool falls back to the
// task ID in that case, so the "name" label does the same.
func taskName(name string, taskID uuid.UUID) string {
	if name == "" {
		return taskID.String()
	}
	return name
}

func orUnset(v string) string {
	if v == "" {
		return PropertyUnset
	}
	return v
}

// BeginRun updates "run_indicator", "task_state" and "task_run_start_seconds".
func (m SchedulerMetrics) BeginRun(clusterID uuid.UUID, taskType string, taskID uuid.UUID, startTime int64) {
	m.runIndicator.WithLabelValues(clusterID.String(), taskType, taskID.String()).Inc()
	m.SetTaskState(clusterID, taskType, taskID, TaskStateRunning)
	m.SetTaskRunStart(clusterID, taskType, taskID, startTime)
}

// EndRun updates "run_indicator", "runs_total" and "last_success".
// The task state is set by the caller, which owns the task statuses and
// their mapping to the reported state.
func (m SchedulerMetrics) EndRun(clusterID uuid.UUID, taskType string, taskID uuid.UUID, status string, startTime int64) {
	m.runIndicator.WithLabelValues(clusterID.String(), taskType, taskID.String()).Dec()
	m.runsTotal.WithLabelValues(clusterID.String(), taskType, taskID.String(), status).Inc()
	if status == "DONE" {
		m.SetLastSuccess(clusterID, taskType, taskID, startTime)
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
