// Copyright (C) 2017 ScyllaDB

package metrics

import (
	"github.com/prometheus/client_golang/prometheus"

	"github.com/scylladb/scylla-manager/v3/pkg/service/backup/backupspec"
	"github.com/scylladb/scylla-manager/v3/pkg/util/uuid"
)

type BackupMetrics struct {
	Backup  BackupM
	Restore RestoreM
}

func NewBackupMetrics() BackupMetrics {
	gb := gaugeVecCreator("backup")
	gr := gaugeVecCreator("restore")

	return BackupMetrics{
		Backup: BackupM{
			snapshot: gb("Indicates if snapshot was taken.",
				"snapshot", "cluster", "keyspace", "host"),
			filesSizeBytes: gb("Total size of backup files in bytes.",
				"files_size_bytes", "cluster", "keyspace", "table", "host"),
			filesUploadedBytes: gb("Number of bytes uploaded to backup location.",
				"files_uploaded_bytes", "cluster", "keyspace", "table", "host"),
			filesSkippedBytes: gb("Number of deduplicated bytes already uploaded to backup location.",
				"files_skipped_bytes", "cluster", "keyspace", "table", "host"),
			filesFailedBytes: gb("Number of bytes failed to upload to backup location.",
				"files_failed_bytes", "cluster", "keyspace", "table", "host"),
			purgeFiles: gb("Number of files that need to be deleted due to retention policy.",
				"purge_files", "cluster", "host"),
			purgeDeletedFiles: gb("Number of files that were deleted.",
				"purge_deleted_files", "cluster", "host"),
		},
		Restore: RestoreM{
			batchSize: gr("Cumulative size of the batches of files taken by the host to restore the data.", "batch_size", "cluster", "host"),
			remainingBytes: gr("Remaining bytes of backup to be restored yet.", "remaining_bytes",
				"cluster", "snapshot_tag", "location", "dc", "node", "keyspace", "table"),
			state:           gr("Defines current state of the restore process (idle/download/load/error).", "state", "cluster", "location", "snapshot_tag", "host"),
			progress:        gr("Defines current progress of the restore process.", "progress", "cluster", "snapshot_tag"),
			viewBuildStatus: gr("Defines build status of recreated view.", "view_build_status", "cluster", "keyspace", "view"),
		},
	}
}

func (m BackupMetrics) all() []prometheus.Collector {
	var c []prometheus.Collector
	c = append(c, m.Backup.all()...)
	c = append(c, m.Restore.all()...)
	return c
}

// MustRegister shall be called to make the metrics visible by prometheus client.
func (m BackupMetrics) MustRegister() BackupMetrics {
	prometheus.MustRegister(m.all()...)
	return m
}

// BackupM is the part of BackupMetrics that is responsible only for backup.
type BackupM struct {
	snapshot           *prometheus.GaugeVec
	filesSizeBytes     *prometheus.GaugeVec
	filesUploadedBytes *prometheus.GaugeVec
	filesSkippedBytes  *prometheus.GaugeVec
	filesFailedBytes   *prometheus.GaugeVec
	purgeFiles         *prometheus.GaugeVec
	purgeDeletedFiles  *prometheus.GaugeVec
}

func (bm BackupM) all() []prometheus.Collector {
	return []prometheus.Collector{
		bm.snapshot,
		bm.filesSizeBytes,
		bm.filesUploadedBytes,
		bm.filesSkippedBytes,
		bm.filesFailedBytes,
		bm.purgeFiles,
		bm.purgeDeletedFiles,
	}
}

// ResetClusterMetrics resets all backup metrics labeled with the cluster.
func (bm BackupM) ResetClusterMetrics(clusterID uuid.UUID) {
	for _, c := range bm.all() {
		setGaugeVecMatching(c.(*prometheus.GaugeVec), unspecifiedValue, clusterMatcher(clusterID))
	}
}

// SetSnapshot updates backup "snapshot" metric.
func (bm BackupM) SetSnapshot(clusterID uuid.UUID, keyspace, host string, taken bool) {
	l := prometheus.Labels{
		"cluster":  clusterID.String(),
		"keyspace": keyspace,
		"host":     host,
	}
	v := 0.
	if taken {
		v = 1
	}
	bm.snapshot.With(l).Set(v)
}

// SetFilesProgress updates backup "files_{uploaded,skipped,failed}_bytes" metrics.
func (bm BackupM) SetFilesProgress(clusterID uuid.UUID, keyspace, table, host string, size, uploaded, skipped, failed int64) {
	l := prometheus.Labels{
		"cluster":  clusterID.String(),
		"keyspace": keyspace,
		"table":    table,
		"host":     host,
	}
	bm.filesSizeBytes.With(l).Set(float64(size))
	bm.filesUploadedBytes.With(l).Set(float64(uploaded))
	bm.filesSkippedBytes.With(l).Set(float64(skipped))
	bm.filesFailedBytes.With(l).Set(float64(failed))
}

// SetPurgeFiles updates backup "purge_files" and "purge_deleted_files" metrics.
func (bm BackupM) SetPurgeFiles(clusterID uuid.UUID, host string, total, deleted int) {
	bm.purgeFiles.WithLabelValues(clusterID.String(), host).Set(float64(total))
	bm.purgeDeletedFiles.WithLabelValues(clusterID.String(), host).Set(float64(deleted))
}

// RestoreM is the part of BackupMetrics that is only responsible for restore.
type RestoreM struct {
	batchSize       *prometheus.GaugeVec
	remainingBytes  *prometheus.GaugeVec
	state           *prometheus.GaugeVec
	progress        *prometheus.GaugeVec
	viewBuildStatus *prometheus.GaugeVec
}

func (rm RestoreM) all() []prometheus.Collector {
	return []prometheus.Collector{
		rm.batchSize,
		rm.remainingBytes,
		rm.state,
		rm.progress,
		rm.viewBuildStatus,
	}
}

// ResetClusterMetrics resets all restore metrics labeled with the cluster.
func (rm RestoreM) ResetClusterMetrics(clusterID uuid.UUID) {
	for _, c := range rm.all() {
		setGaugeVecMatching(c.(*prometheus.GaugeVec), unspecifiedValue, clusterMatcher(clusterID))
	}
}

// IncreaseBatchSize updates restore "batch_size" metrics.
func (rm RestoreM) IncreaseBatchSize(clusterID uuid.UUID, host string, size int64) {
	l := prometheus.Labels{
		"cluster": clusterID.String(),
		"host":    host,
	}

	rm.batchSize.With(l).Add(float64(size))
}

// DecreaseBatchSize updates restore "batch_size" metrics.
func (rm RestoreM) DecreaseBatchSize(clusterID uuid.UUID, host string, size int64) {
	l := prometheus.Labels{
		"cluster": clusterID.String(),
		"host":    host,
	}

	rm.batchSize.With(l).Sub(float64(size))
}

// SetRemainingBytes sets restore "remaining_bytes" metric.
func (rm RestoreM) SetRemainingBytes(labels RestoreBytesLabels, remainingBytes int64) {
	l := prometheus.Labels{
		"cluster":      labels.ClusterID,
		"snapshot_tag": labels.SnapshotTag,
		"location":     labels.Location,
		"dc":           labels.DC,
		"node":         labels.Node,
		"keyspace":     labels.Keyspace,
		"table":        labels.Table,
	}
	rm.remainingBytes.With(l).Set(float64(remainingBytes))
}

// RestoreBytesLabels is a set of labels for restore metrics.
type RestoreBytesLabels struct {
	ClusterID   string
	SnapshotTag string
	Location    string
	DC          string
	Node        string
	Keyspace    string
	Table       string
}

// DecreaseRemainingBytes decreases restore "remaining_bytes" metric.
func (rm RestoreM) DecreaseRemainingBytes(labels RestoreBytesLabels, restoredBytes int64) {
	l := prometheus.Labels{
		"cluster":      labels.ClusterID,
		"snapshot_tag": labels.SnapshotTag,
		"location":     labels.Location,
		"dc":           labels.DC,
		"node":         labels.Node,
		"keyspace":     labels.Keyspace,
		"table":        labels.Table,
	}
	rm.remainingBytes.With(l).Sub(float64(restoredBytes))
}

// RestoreProgressLabels is a set of labels for restore "progress" metric.
// RestoreProgressLabels does not contain DC and Node labels since we only care about global restore progress.
type RestoreProgressLabels struct {
	ClusterID   string
	SnapshotTag string
}

// SetProgress sets restore "progress" metric,
// progress should be a value between 0 and 100, that indicates global restore progress.
func (rm RestoreM) SetProgress(labels RestoreProgressLabels, progress float64) {
	l := prometheus.Labels{
		"cluster":      labels.ClusterID,
		"snapshot_tag": labels.SnapshotTag,
	}
	rm.progress.With(l).Set(progress)
}

// RestoreState is the enum that defines how node is used during the restore.
type RestoreState int

const (
	// RestoreStateIdle defines idle state.
	RestoreStateIdle RestoreState = iota
	// RestoreStateDownloading means that node is downloading data from backup location.
	RestoreStateDownloading
	// RestoreStateLoading means that node is calling load&stream.
	RestoreStateLoading
	// RestoreStateError means that node ended up with error.
	RestoreStateError
)

// SetRestoreState sets restore "state" metric.
func (rm RestoreM) SetRestoreState(clusterID uuid.UUID, location backupspec.Location, snapshotTag, host string, state RestoreState) {
	l := prometheus.Labels{
		"cluster":      clusterID.String(),
		"location":     location.String(),
		"snapshot_tag": snapshotTag,
		"host":         host,
	}
	rm.state.With(l).Set(float64(state))
}

// ViewBuildStatus defines build status of a view.
type ViewBuildStatus int

// ViewBuildStatus enumeration.
const (
	BuildStatusUnknown ViewBuildStatus = iota
	BuildStatusStarted
	BuildStatusSuccess
	BuildStatusError
)

// RestoreViewBuildStatusLabels is a set of labels for restore "view_build_status" metric.
type RestoreViewBuildStatusLabels struct {
	ClusterID string
	Keyspace  string
	View      string
}

// SetViewBuildStatus sets restore "view_build_status" metric.
func (rm RestoreM) SetViewBuildStatus(labels RestoreViewBuildStatusLabels, status ViewBuildStatus) {
	l := prometheus.Labels{
		"cluster":  labels.ClusterID,
		"keyspace": labels.Keyspace,
		"view":     labels.View,
	}
	rm.viewBuildStatus.With(l).Set(float64(status))
}
