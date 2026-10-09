// Copyright (C) 2017 ScyllaDB

package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/scylladb/scylla-manager/v3/pkg/util/uuid"
)

// RepairTaskProperties describes the configured properties of a repair task
// reported by the "repair_task_properties" metric. They are the task
// properties as the repair service resolved them, so the Scylla Manager side
// defaults are already filled in.
type RepairTaskProperties struct {
	Keyspace            string
	DC                  string
	KeyspaceReplication string
	IncrementalMode     string
	Host                string
	FailFast            string
}

type RepairMetrics struct {
	taskProperties      *prometheus.GaugeVec
	progress            *prometheus.GaugeVec
	taskProgress        *prometheus.GaugeVec
	tokenRangesTotal    *prometheus.GaugeVec
	tokenRangesSuccess  *prometheus.GaugeVec
	tokenRangesError    *prometheus.GaugeVec
	inFlightJobs        *prometheus.GaugeVec
	inFlightTokenRanges *prometheus.GaugeVec
}

func NewRepairMetrics() RepairMetrics {
	g := gaugeVecCreator("repair")

	return RepairMetrics{
		taskProperties: g("Configured properties of a repair task, always 1. "+
			"Join it to the other task metrics on the \"task\" label. "+
			"The name and the schedule of the task are reported by "+
			"\"scylla_manager_scheduler_task_properties\". "+
			"A property that was not configured and has no Scylla Manager side "+
			"default is reported as \"-\" - the incremental mode is one, because "+
			"Scylla Manager leaves it to Scylla.",
			"task_properties", "cluster", "task", "keyspace", "dc",
			"keyspace_replication", "incremental_mode", "host", "fail_fast"),
		progress: g("Total percentage repair progress.", "progress", "cluster"),
		taskProgress: g("Repair task progress in percents (0-100). "+
			"It describes the repair task - the tablet repair task reports its own "+
			"progress as \"scylla_manager_tablet_repair_progress\".",
			"task_progress", "cluster", "task"),
		tokenRangesTotal: g("Total number of token ranges to repair.",
			"token_ranges_total", "cluster", "keyspace", "table", "host"),
		tokenRangesSuccess: g("Number of repaired token ranges.",
			"token_ranges_success", "cluster", "keyspace", "table", "host"),
		tokenRangesError: g("Number of segments that failed to repair.",
			"token_ranges_error", "cluster", "keyspace", "table", "host"),
		inFlightJobs: g("Number of currently running Scylla repair jobs.",
			"inflight_jobs", "cluster", "host"),
		inFlightTokenRanges: g("Number of token ranges that are being repaired.",
			"inflight_token_ranges", "cluster", "host"),
	}
}

func (m RepairMetrics) all() []prometheus.Collector {
	return []prometheus.Collector{
		m.taskProperties,
		m.progress,
		m.taskProgress,
		m.tokenRangesTotal,
		m.tokenRangesSuccess,
		m.tokenRangesError,
		m.inFlightJobs,
		m.inFlightTokenRanges,
	}
}

// MustRegister shall be called to make the metrics visible by prometheus client.
func (m RepairMetrics) MustRegister() RepairMetrics {
	prometheus.MustRegister(m.all()...)
	return m
}

// ResetClusterMetrics resets all metrics labeled with the cluster.
func (m RepairMetrics) ResetClusterMetrics(clusterID uuid.UUID) {
	for _, c := range m.all() {
		if c == prometheus.Collector(m.taskProgress) {
			// "task_progress" is scoped to a single task, while this method
			// is called at the beginning of every repair run. Resetting it
			// here would wipe the progress of every other repair task of
			// the cluster. Each task overwrites its own series on run start.
			continue
		}
		setGaugeVecMatching(c.(*prometheus.GaugeVec), unspecifiedValue, clusterMatcher(clusterID))
	}
}

// SetTokenRanges updates "token_ranges_{total,success,error}" metrics.
func (m RepairMetrics) SetTokenRanges(clusterID uuid.UUID, keyspace, table, host string, total, success, errcnt int64) {
	l := prometheus.Labels{
		"cluster":  clusterID.String(),
		"keyspace": keyspace,
		"table":    table,
		"host":     host,
	}
	m.tokenRangesTotal.With(l).Set(float64(total))
	m.tokenRangesSuccess.With(l).Set(float64(success))
	m.tokenRangesError.With(l).Set(float64(errcnt))
}

// AddJob updates "inflight_{jobs,token_ranges}" metrics.
func (m RepairMetrics) AddJob(clusterID uuid.UUID, host string, tokenRanges int) {
	l := prometheus.Labels{
		"cluster": clusterID.String(),
		"host":    host,
	}
	m.inFlightJobs.With(l).Add(1)
	m.inFlightTokenRanges.With(l).Add(float64(tokenRanges))
}

// SubJob updates "inflight_{jobs,token_ranges}" metrics.
func (m RepairMetrics) SubJob(clusterID uuid.UUID, host string, tokenRanges int) {
	l := prometheus.Labels{
		"cluster": clusterID.String(),
		"host":    host,
	}
	m.inFlightJobs.With(l).Sub(1)
	m.inFlightTokenRanges.With(l).Sub(float64(tokenRanges))
}

// SetProgress sets "progress" metric.
func (m RepairMetrics) SetProgress(clusterID uuid.UUID, progress float64) {
	l := prometheus.Labels{
		"cluster": clusterID.String(),
	}
	m.progress.With(l).Set(progress)
}

// SetTaskProgress sets "task_progress" metric.
func (m RepairMetrics) SetTaskProgress(clusterID, taskID uuid.UUID, progress float64) {
	l := prometheus.Labels{
		"cluster": clusterID.String(),
		"task":    taskID.String(),
	}
	m.taskProgress.With(l).Set(progress)
}

// SetTaskProperties updates "task_properties" with the resolved properties of
// the task. See the backup metric of the same name.
func (m RepairMetrics) SetTaskProperties(clusterID, taskID uuid.UUID, p RepairTaskProperties) {
	m.taskProperties.DeletePartialMatch(prometheus.Labels{"task": taskID.String()})
	m.taskProperties.WithLabelValues(
		clusterID.String(), taskID.String(), orUnset(p.Keyspace), orUnset(p.DC),
		orUnset(p.KeyspaceReplication), orUnset(p.IncrementalMode),
		orUnset(p.Host), orUnset(p.FailFast),
	).Set(1)
}

// ResetTaskMetrics resets the metrics of a single repair task.
// It is called when the task starts, so that the progress of its previous
// run does not linger until the new run reports for the first time.
func (m RepairMetrics) ResetTaskMetrics(clusterID, taskID uuid.UUID) {
	m.SetTaskProgress(clusterID, taskID, 0)
}

// AddProgress updates "progress" metric.
func (m RepairMetrics) AddProgress(clusterID uuid.UUID, delta float64) {
	l := prometheus.Labels{
		"cluster": clusterID.String(),
	}
	m.progress.With(l).Add(delta)
}
