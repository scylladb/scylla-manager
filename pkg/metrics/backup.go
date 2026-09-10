// Copyright (C) 2026 ScyllaDB

package metrics

import (
	"github.com/prometheus/client_golang/prometheus"

	"github.com/scylladb/scylla-manager/v3/pkg/util/uuid"
)

// Backup types reported by the per task backup metrics.
// They mirror the backup method configured for the task. "auto" means that
// the method is resolved per node and table, so the task can use both.
const (
	// BackupTypeRclone describes a backup done with the rclone API.
	BackupTypeRclone = "rclone"
	// BackupTypeNative describes a backup done with the native Scylla API.
	BackupTypeNative = "native"
	// BackupTypeAuto describes a backup that picks the method per table.
	BackupTypeAuto = "auto"
)

type BackupMetrics struct {
	taskProgress           *prometheus.GaugeVec
	taskFilesSizeBytes     *prometheus.GaugeVec
	taskFilesUploadedBytes *prometheus.GaugeVec
	taskFilesSkippedBytes  *prometheus.GaugeVec

	// Deprecated: use "backup_task_progress" instead.
	snapshot           *prometheus.GaugeVec
	filesSizeBytes     *prometheus.GaugeVec
	filesUploadedBytes *prometheus.GaugeVec
	filesSkippedBytes  *prometheus.GaugeVec
	filesFailedBytes   *prometheus.GaugeVec
	// Deprecated: use "backup_task_progress" instead.
	purgeFiles *prometheus.GaugeVec
	// Deprecated: use "backup_task_progress" instead.
	purgeDeletedFiles *prometheus.GaugeVec
	// Deprecated: use "backup_task_progress" instead.
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
		taskProgress: g("Backup task progress in percents (0-100), "+
			"calculated as (uploaded + skipped) / size. "+
			"The \"backup_type\" label describes the configured backup method - "+
			"\"auto\" means that the method is resolved per node and table.",
			"task_progress", "cluster", "task", "backup_type"),
		taskFilesSizeBytes: g("Total size of the backed up files in bytes, per node.",
			"task_files_size_bytes", "cluster", "task", "backup_type", "host"),
		taskFilesUploadedBytes: g("Number of bytes uploaded to the backup location, per node.",
			"task_files_uploaded_bytes", "cluster", "task", "backup_type", "host"),
		taskFilesSkippedBytes: g("Number of deduplicated bytes already present "+
			"in the backup location, per node.",
			"task_files_skipped_bytes", "cluster", "task", "backup_type", "host"),
		snapshot: g("DEPRECATED: Use scylla_manager_backup_task_* instead. Indicates if snapshot was taken.",
			"snapshot", "cluster", "keyspace", "host"),
		filesSizeBytes: g("Total size of backup files in bytes.",
			"files_size_bytes", "cluster", "keyspace", "table", "host"),
		filesUploadedBytes: g("Number of bytes uploaded to backup location.",
			"files_uploaded_bytes", "cluster", "keyspace", "table", "host"),
		filesSkippedBytes: g("Number of deduplicated bytes already uploaded to backup location.",
			"files_skipped_bytes", "cluster", "keyspace", "table", "host"),
		filesFailedBytes: g("Number of bytes failed to upload to backup location.",
			"files_failed_bytes", "cluster", "keyspace", "table", "host"),
		purgeFiles: g("DEPRECATED: Use scylla_manager_backup_task_* instead. Number of files that need to be deleted due to retention policy.",
			"purge_files", "cluster", "host"),
		purgeDeletedFiles: g("DEPRECATED: Use scylla_manager_backup_task_* instead. Number of files that were deleted.",
			"purge_deleted_files", "cluster", "host"),
		retentionLockedFiles: g("DEPRECATED: Use scylla_manager_backup_task_* instead. Number of backup files that had retention lock set.",
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
		m.taskProgress,
		m.taskFilesSizeBytes,
		m.taskFilesUploadedBytes,
		m.taskFilesSkippedBytes,
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

// ResetTaskMetrics clears the per node byte metrics of a task and sets its
// progress to 0. It is called when a run starts, so that the run does not
// begin by reporting the values the previous one finished with - the byte
// metrics are absolute gauges, and until the first progress update overwrites
// them they still describe the previous run, which reads as 100% complete.
// Deleting rather than zeroing also drops nodes that are no longer part of
// the backup instead of leaving them at their last value forever.
func (m BackupMetrics) ResetTaskMetrics(clusterID, taskID uuid.UUID, backupType string) {
	matcher := LabelMatcher("task", taskID.String())
	for _, c := range []*prometheus.GaugeVec{
		m.taskFilesSizeBytes,
		m.taskFilesUploadedBytes,
		m.taskFilesSkippedBytes,
	} {
		DeleteMatching(c, matcher)
	}
	m.SetTaskProgress(clusterID, taskID, backupType, 0)
}

// SetTaskProgress updates backup "task_progress" metric.
func (m BackupMetrics) SetTaskProgress(clusterID, taskID uuid.UUID, backupType string, progress float64) {
	l := prometheus.Labels{
		"cluster":     clusterID.String(),
		"task":        taskID.String(),
		"backup_type": backupType,
	}
	m.taskProgress.With(l).Set(progress)
}

// SetTaskFilesProgress updates backup
// "task_files_{size,uploaded,skipped}_bytes" metrics of a single node.
func (m BackupMetrics) SetTaskFilesProgress(clusterID, taskID uuid.UUID, backupType, host string,
	size, uploaded, skipped int64,
) {
	l := prometheus.Labels{
		"cluster":     clusterID.String(),
		"task":        taskID.String(),
		"backup_type": backupType,
		"host":        host,
	}
	m.taskFilesSizeBytes.With(l).Set(float64(size))
	m.taskFilesUploadedBytes.With(l).Set(float64(uploaded))
	m.taskFilesSkippedBytes.With(l).Set(float64(skipped))
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
