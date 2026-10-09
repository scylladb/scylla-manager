// Copyright (C) 2026 ScyllaDB

package metrics

import (
	"github.com/prometheus/client_golang/prometheus"

	"github.com/scylladb/scylla-manager/v3/pkg/util/uuid"
)

// BackupTaskProperties describes the configured properties of a backup task
// reported by the "backup_task_properties" metric. They are the task
// properties as the backup service resolved them, so the Scylla Manager side
// defaults are already filled in.
type BackupTaskProperties struct {
	Keyspace          string
	DC                string
	Location          string
	Retention         string
	RetentionDays     string
	Method            string
	PurgeOnly         string
	SkipSchema        string
	RetentionLockMode string
}

type BackupMetrics struct {
	taskProperties         *prometheus.GaugeVec
	taskProgress           *prometheus.GaugeVec
	snapshot               *prometheus.GaugeVec
	filesSizeBytes         *prometheus.GaugeVec
	filesUploadedBytes     *prometheus.GaugeVec
	filesSkippedBytes      *prometheus.GaugeVec
	filesFailedBytes       *prometheus.GaugeVec
	purgeFiles             *prometheus.GaugeVec
	purgeDeletedFiles      *prometheus.GaugeVec
	retentionLockedFiles   *prometheus.GaugeVec
	filesCount             *prometheus.GaugeVec
	filesSkippedCount      *prometheus.GaugeVec
	versionedFilesCount    *prometheus.GaugeVec
	setEventBasedHolds     *prometheus.GaugeVec
	removedEventBasedHolds *prometheus.GaugeVec
}

func NewBackupMetrics() BackupMetrics {
	g := gaugeVecCreator("backup")

	return BackupMetrics{
		taskProperties: g("Configured properties of a backup task, always 1. "+
			"Join it to the other task metrics on the \"task\" label. "+
			"The name and the schedule of the task are reported by "+
			"\"scylla_manager_scheduler_task_properties\". "+
			"A property that was not configured and has no Scylla Manager side "+
			"default is reported as \"-\".",
			"task_properties", "cluster", "task", "keyspace", "dc", "location",
			"retention", "retention_days", "method", "purge_only", "skip_schema",
			"retention_lock_mode"),
		taskProgress: g("Backup task progress in percents (0-100), "+
			"calculated as (uploaded + skipped) / size over every backed up table.",
			"task_progress", "cluster", "task"),
		snapshot: g("Indicates if snapshot was taken.",
			"snapshot", "cluster", "keyspace", "host"),
		filesSizeBytes: g("Total size of backup files in bytes.",
			"files_size_bytes", "cluster", "keyspace", "table", "host"),
		filesUploadedBytes: g("Number of bytes uploaded to backup location.",
			"files_uploaded_bytes", "cluster", "keyspace", "table", "host"),
		filesSkippedBytes: g("Number of deduplicated bytes already uploaded to backup location.",
			"files_skipped_bytes", "cluster", "keyspace", "table", "host"),
		filesFailedBytes: g("Number of bytes failed to upload to backup location.",
			"files_failed_bytes", "cluster", "keyspace", "table", "host"),
		purgeFiles: g("Number of files that need to be deleted due to retention policy.",
			"purge_files", "cluster", "host"),
		purgeDeletedFiles: g("Number of files that were deleted.",
			"purge_deleted_files", "cluster", "host"),
		retentionLockedFiles: g("Number of backup files that had retention lock set.",
			"retention_locked_files", "cluster", "keyspace", "table", "host"),
		filesCount: g("Number of snapshot files before deduplication.",
			"files_count", "cluster", "keyspace", "table", "host"),
		filesSkippedCount: g("Number of deduplicated snapshot files already uploaded to backup location.",
			"files_skipped_count", "cluster", "keyspace", "table", "host"),
		versionedFilesCount: g("Number of versioned snapshot files that will be created on snapshot upload.",
			"versioned_files_count", "cluster", "node", "keyspace", "table"),
		setEventBasedHolds: g("Number of snapshot files that had event based hold set. "+
			"The \"node\" label describes the ID of the node owning the file, "+
			"while the \"host\" label describes the IP of the node setting the hold.",
			"set_event_based_holds", "cluster", "node", "keyspace", "table", "host"),
		removedEventBasedHolds: g("Number of snapshot files that had event based hold removed. "+
			"The \"node\" label describes the ID of the node owning the file, "+
			"while the \"host\" label describes the IP of the node setting the hold.",
			"removed_event_based_holds", "cluster", "node", "keyspace", "table", "host"),
	}
}

// MustRegister shall be called to make the metrics visible by prometheus client.
func (m BackupMetrics) MustRegister() BackupMetrics {
	return m.MustRegisterWith(prometheus.DefaultRegisterer)
}

// MustRegisterWith registers all backup metrics with the given registerer.
func (m BackupMetrics) MustRegisterWith(reg prometheus.Registerer) BackupMetrics {
	reg.MustRegister(m.all()...)
	return m
}

func (m BackupMetrics) all() []prometheus.Collector {
	return []prometheus.Collector{
		m.taskProperties,
		m.taskProgress,
		m.snapshot,
		m.filesSizeBytes,
		m.filesUploadedBytes,
		m.filesSkippedBytes,
		m.filesFailedBytes,
		m.purgeFiles,
		m.purgeDeletedFiles,
		m.retentionLockedFiles,
		m.filesCount,
		m.filesSkippedCount,
		m.versionedFilesCount,
		m.setEventBasedHolds,
		m.removedEventBasedHolds,
	}
}

// ResetClusterMetrics resets all backup metrics labeled with the cluster.
func (m BackupMetrics) ResetClusterMetrics(clusterID uuid.UUID) {
	for _, c := range []*prometheus.GaugeVec{
		m.snapshot,
		m.filesSizeBytes,
		m.filesUploadedBytes,
		m.filesSkippedBytes,
		m.filesFailedBytes,
		m.purgeFiles,
		m.purgeDeletedFiles,
		m.retentionLockedFiles,
		m.filesCount,
		m.filesSkippedCount,
	} {
		setGaugeVecMatching(c, unspecifiedValue, clusterMatcher(clusterID))
	}
	// Newer metrics are deleted instead of being set to unspecifiedValue,
	// so that series of nodes and tables that are no longer part of
	// the cluster aren't reported indefinitely.
	for _, c := range []*prometheus.GaugeVec{
		m.versionedFilesCount,
		m.setEventBasedHolds,
		m.removedEventBasedHolds,
	} {
		DeleteMatching(c, clusterMatcher(clusterID))
	}
}

// SetTaskProperties updates "task_properties" with the resolved properties of
// the task. The previous series of the task is removed first, so that editing
// a property replaces it instead of leaving a stale one behind.
func (m BackupMetrics) SetTaskProperties(clusterID, taskID uuid.UUID, p BackupTaskProperties) {
	m.taskProperties.DeletePartialMatch(prometheus.Labels{"task": taskID.String()})
	m.taskProperties.WithLabelValues(
		clusterID.String(), taskID.String(), orUnset(p.Keyspace), orUnset(p.DC),
		orUnset(p.Location), orUnset(p.Retention), orUnset(p.RetentionDays),
		orUnset(p.Method), orUnset(p.PurgeOnly), orUnset(p.SkipSchema),
		orUnset(p.RetentionLockMode),
	).Set(1)
}

// ResetTaskMetrics resets the metrics of a single backup task.
// It is called when the task starts, so that the progress of its previous
// run does not linger until the new run reports for the first time.
func (m BackupMetrics) ResetTaskMetrics(clusterID, taskID uuid.UUID) {
	m.SetTaskProgress(clusterID, taskID, 0)
}

// SetTaskProgress updates backup "task_progress" metric.
func (m BackupMetrics) SetTaskProgress(clusterID, taskID uuid.UUID, progress float64) {
	l := prometheus.Labels{
		"cluster": clusterID.String(),
		"task":    taskID.String(),
	}
	m.taskProgress.With(l).Set(progress)
}

// SetSnapshot updates backup "snapshot" metric.
func (m BackupMetrics) SetSnapshot(clusterID uuid.UUID, keyspace, host string, taken bool) {
	l := prometheus.Labels{
		"cluster":  clusterID.String(),
		"keyspace": keyspace,
		"host":     host,
	}
	v := 0.
	if taken {
		v = 1
	}
	m.snapshot.With(l).Set(v)
}

// SetFilesProgress updates backup "files_{size,count,uploaded,skipped,skipped_count,failed}_bytes" metrics.
func (m BackupMetrics) SetFilesProgress(clusterID uuid.UUID, keyspace, table, host string,
	size, uploaded, skipped, failed, filesCount, filesSkippedCount int64,
) {
	l := prometheus.Labels{
		"cluster":  clusterID.String(),
		"keyspace": keyspace,
		"table":    table,
		"host":     host,
	}
	m.filesSizeBytes.With(l).Set(float64(size))
	m.filesUploadedBytes.With(l).Set(float64(uploaded))
	m.filesSkippedBytes.With(l).Set(float64(skipped))
	m.filesFailedBytes.With(l).Set(float64(failed))
	m.filesCount.With(l).Set(float64(filesCount))
	m.filesSkippedCount.With(l).Set(float64(filesSkippedCount))
}

// SetPurgeFiles updates backup "purge_files" and "purge_deleted_files" metrics.
func (m BackupMetrics) SetPurgeFiles(clusterID uuid.UUID, host string, total, deleted int) {
	m.purgeFiles.WithLabelValues(clusterID.String(), host).Set(float64(total))
	m.purgeDeletedFiles.WithLabelValues(clusterID.String(), host).Set(float64(deleted))
}

// IncreaseRetentionLockedFiles increases backup "retention_locked_files" metric.
func (m BackupMetrics) IncreaseRetentionLockedFiles(clusterID uuid.UUID, keyspace, table, host string, locked int64) {
	l := prometheus.Labels{
		"cluster":  clusterID.String(),
		"keyspace": keyspace,
		"table":    table,
		"host":     host,
	}
	m.retentionLockedFiles.With(l).Add(float64(locked))
}

// SetVersionedFilesCount updates backup "versioned_files_count" metric.
func (m BackupMetrics) SetVersionedFilesCount(clusterID uuid.UUID, nodeID, keyspace, table string, count int) {
	l := prometheus.Labels{
		"cluster":  clusterID.String(),
		"node":     nodeID,
		"keyspace": keyspace,
		"table":    table,
	}
	m.versionedFilesCount.With(l).Set(float64(count))
}

// IncreaseEventBasedHolds increases backup "set_event_based_holds" (hold=true)
// or "removed_event_based_holds" (hold=false) metric.
// The "node" label describes the ID of the node owning the file,
// while the "host" label describes the IP of the node setting the hold.
func (m BackupMetrics) IncreaseEventBasedHolds(clusterID uuid.UUID, nodeID, keyspace, table, host string, hold bool, count int64) {
	l := prometheus.Labels{
		"cluster":  clusterID.String(),
		"node":     nodeID,
		"keyspace": keyspace,
		"table":    table,
		"host":     host,
	}
	if hold {
		m.setEventBasedHolds.With(l).Add(float64(count))
	} else {
		m.removedEventBasedHolds.With(l).Add(float64(count))
	}
}
