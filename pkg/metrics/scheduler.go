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
	taskState    *prometheus.GaugeVec

	taskRunStartSeconds   *prometheus.GaugeVec
	taskLastSuccessSecond *prometheus.GaugeVec
	taskInfo              *prometheus.GaugeVec
}

// TaskInfo describes the configured task properties reported by the
// "task_info" metric. Only properties that were actually set are reported -
// an unset property is reported as TaskInfoUnset, so that the metric never
// claims a value the user did not configure.
type TaskInfo struct {
	Name string
	Cron string
	// Keyspace and DC are configured by both repair and backup tasks.
	Keyspace string
	DC       string
	// Repair only.
	KeyspaceReplication string
	IncrementalMode     string
	Host                string
	FailFast            string
	// Backup only.
	Location          string
	Retention         string
	RetentionDays     string
	Method            string
	PurgeOnly         string
	SkipSchema        string
	RetentionLockMode string
}

// TaskInfoUnset is reported for a property that was not configured.
// An empty label value cannot be used, because Prometheus treats an empty
// label as an absent one, which would drop the property from the series
// entirely and make it impossible to render as a fixed table column.
const TaskInfoUnset = "-"

// TaskScheduleAdHoc is reported in the "cron" label of a task that is not
// scheduled at all. It sorts after any cron specification, so a list
// ordered by schedule keeps ad-hoc tasks at the end.
const TaskScheduleAdHoc = "ad-hoc"

// taskInfoLabels lists the "task_info" labels in the order they are set.
// A property that does not apply to a task type is reported as TaskInfoUnset,
// so that every task has the same set of labels and a dashboard can render a
// fixed set of columns per type.
var taskInfoLabels = []string{
	"cluster", "type", "task", "name", "cron", "keyspace", "dc",
	// Repair only.
	"keyspace_replication", "incremental_mode", "host", "fail_fast",
	// Backup only.
	"location", "retention", "retention_days", "method", "purge_only",
	"skip_schema", "retention_lock_mode",
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
		taskState: g("State of the last known task run: "+
			"0 - never run, 1 - running, 2 - done, 3 - error, 4 - stopped. "+
			"The value is latched, so it describes the last run until the next one starts. "+
			"The \"type\" label of tablet repair tasks is reported as \"repair\", "+
			"so that filtering by type=\"repair\" returns both vnode and tablet repair tasks.",
			"task_state", "cluster", "type", "task"),
		taskRunStartSeconds: g("Start time of the last task run as a Unix timestamp. "+
			"Together with \"task_state\" it tells for how long a task has been running.",
			"task_run_start_seconds", "cluster", "type", "task"),
		taskLastSuccessSecond: g("End time of the last successful task run as a Unix timestamp. "+
			"Unlike \"last_success\", which reports the start time of that run, "+
			"this metric reports when the run actually finished, "+
			"so that \"time() - task_last_success_seconds\" is the real age of the last success.",
			"task_last_success_seconds", "cluster", "type", "task"),
		taskInfo: g("Configured properties of a task, always 1. "+
			"Join it to other task metrics on the \"task\" label, e.g. "+
			"\"task_state * on(task) group_left(keyspace) task_info\". "+
			"A property that was not configured, or that does not apply to the "+
			"task type, is reported as \"-\". "+
			"The \"name\" label falls back to the task ID for unnamed tasks.",
			"task_info", taskInfoLabels...),
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
		m.taskLastSuccessSecond,
		m.taskInfo,
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

// InitTaskState restores "task_state" from the last known task status.
// It is needed so that a task that ended in error before SM restart
// is still reported as failed after the restart.
// Statuses that do not describe a finished run are ignored - the metric
// is not reported at all until the task runs for the first time.
func (m SchedulerMetrics) InitTaskState(clusterID uuid.UUID, taskType string, taskID uuid.UUID, status string) {
	state, ok := taskStateFromStatus(status)
	if !ok || state == TaskStateRunning {
		// A task that was running when SM stopped is not running anymore.
		return
	}
	m.taskState.WithLabelValues(clusterID.String(), normalizeTaskType(taskType), taskID.String()).Set(float64(state))
}

// InitTaskRunStart restores "task_run_start_seconds" from the last recorded
// run of the task, so that the metric survives a restart of SM.
func (m SchedulerMetrics) InitTaskRunStart(clusterID uuid.UUID, taskType string, taskID uuid.UUID, startTime int64) {
	m.taskRunStartSeconds.WithLabelValues(clusterID.String(), normalizeTaskType(taskType), taskID.String()).
		Set(float64(startTime))
}

// InitTaskLastSuccess restores "task_last_success_seconds" from the last
// successful run recorded for the task. It is needed so that the age of the
// last success is not reset by an SM restart.
func (m SchedulerMetrics) InitTaskLastSuccess(clusterID uuid.UUID, taskType string, taskID uuid.UUID, endTime int64) {
	m.taskLastSuccessSecond.WithLabelValues(clusterID.String(), normalizeTaskType(taskType), taskID.String()).
		Set(float64(endTime))
}

// DeleteTask removes every series describing the task. It should be called
// when a task is deleted, so that the metrics describe the tasks that
// currently exist - otherwise a deleted task keeps being reported for as
// long as the process runs.
func (m SchedulerMetrics) DeleteTask(taskID uuid.UUID) {
	matcher := LabelMatcher("task", taskID.String())
	for _, c := range []*prometheus.GaugeVec{
		m.runIndicator, m.runsTotal, m.lastSuccess, m.taskState,
		m.taskRunStartSeconds, m.taskLastSuccessSecond, m.taskInfo,
	} {
		DeleteMatching(c, matcher)
	}
}

// SetTaskInfo updates "task_info" with the currently configured properties.
// Previous series of the task are removed first, so that editing a property
// replaces the series instead of leaving a stale one behind.
func (m SchedulerMetrics) SetTaskInfo(clusterID uuid.UUID, taskType string, taskID uuid.UUID, info TaskInfo) {
	DeleteMatching(m.taskInfo, LabelMatcher("task", taskID.String()))
	// A task does not have to be named - an ad-hoc one usually isn't - and
	// sctool falls back to the task ID in that case, so the "name" label does
	// the same. It is always safe to display, unlike the properties.
	name := info.Name
	if name == "" {
		name = taskID.String()
	}
	m.taskInfo.WithLabelValues(
		clusterID.String(), normalizeTaskType(taskType), taskID.String(), name, orUnset(info.Cron),
		orUnset(info.Keyspace), orUnset(info.DC),
		orUnset(info.KeyspaceReplication), orUnset(info.IncrementalMode),
		orUnset(info.Host), orUnset(info.FailFast),
		orUnset(info.Location), orUnset(info.Retention), orUnset(info.RetentionDays),
		orUnset(info.Method), orUnset(info.PurgeOnly), orUnset(info.SkipSchema),
		orUnset(info.RetentionLockMode),
	).Set(1)
}

func orUnset(v string) string {
	if v == "" {
		return TaskInfoUnset
	}
	return v
}

// BeginRun updates "run_indicator", "task_state" and "task_run_start_seconds".
func (m SchedulerMetrics) BeginRun(clusterID uuid.UUID, taskType string, taskID uuid.UUID, startTime int64) {
	m.runIndicator.WithLabelValues(clusterID.String(), taskType, taskID.String()).Inc()
	m.taskState.WithLabelValues(clusterID.String(), normalizeTaskType(taskType), taskID.String()).
		Set(float64(TaskStateRunning))
	m.taskRunStartSeconds.WithLabelValues(clusterID.String(), normalizeTaskType(taskType), taskID.String()).
		Set(float64(startTime))
}

// EndRun updates "run_indicator", "runs_total", "last_success", "task_state"
// and "task_last_success_seconds".
func (m SchedulerMetrics) EndRun(clusterID uuid.UUID, taskType string, taskID uuid.UUID, status string, startTime, endTime int64) {
	m.runIndicator.WithLabelValues(clusterID.String(), taskType, taskID.String()).Dec()
	m.runsTotal.WithLabelValues(clusterID.String(), taskType, taskID.String(), status).Inc()
	if status == statusDone {
		m.lastSuccess.WithLabelValues(clusterID.String(), taskType, taskID.String()).Set(float64(startTime))
		m.InitTaskLastSuccess(clusterID, taskType, taskID, endTime)
	}
	if state, ok := taskStateFromStatus(status); ok {
		m.taskState.WithLabelValues(clusterID.String(), normalizeTaskType(taskType), taskID.String()).
			Set(float64(state))
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
