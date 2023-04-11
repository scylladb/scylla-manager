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
			filesRestoredBytes: gr("Number of bytes restored from downloaded files.",
				"files_restored_bytes", "cluster", "manifest", "keyspace", "table"),
			filesDownloadedBytes: gr("Number of bytes downloaded from backup location (local to current restore run).",
				"files_downloaded_bytes", "cluster", "manifest", "keyspace", "table"),
			filesSkippedBytes: gr("Number of deduplicated bytes already downloaded from backup location (local to current restore run).",
				"files_skipped_bytes", "cluster", "manifest", "keyspace", "table"),
			filesFailedBytes: gr("Number of bytes failed to download from backup location (local to current restore run).",
				"files_failed_bytes", "cluster", "manifest", "keyspace", "table"),
			batchSize: gr("Cumulative size of the batches of files taken by the host to restore the data.", "batch_size", "cluster", "host"),
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
	filesRestoredBytes   *prometheus.GaugeVec
	filesDownloadedBytes *prometheus.GaugeVec
	filesSkippedBytes    *prometheus.GaugeVec
	filesFailedBytes     *prometheus.GaugeVec
	batchSize            *prometheus.GaugeVec
}

func (rm RestoreM) all() []prometheus.Collector {
	return []prometheus.Collector{
		rm.filesRestoredBytes,
		rm.filesDownloadedBytes,
		rm.filesSkippedBytes,
		rm.filesFailedBytes,
		rm.batchSize,
	}
}

// ResetClusterMetrics resets all restore metrics labeled with the cluster.
func (rm RestoreM) ResetClusterMetrics(clusterID uuid.UUID) {
	for _, c := range rm.all() {
		setGaugeVecMatching(c.(*prometheus.GaugeVec), unspecifiedValue, clusterMatcher(clusterID))
	}
}

// UpdateFilesProgress updates restore "files_{downloaded,skipped,failed}_bytes" metrics.
func (rm RestoreM) UpdateFilesProgress(clusterID uuid.UUID, manifestPath, keyspace, table string, downloaded, skipped, failed int64) {
	l := prometheus.Labels{
		"cluster":  clusterID.String(),
		"manifest": manifestPath,
		"keyspace": keyspace,
		"table":    table,
	}

	rm.filesDownloadedBytes.With(l).Add(float64(downloaded))
	rm.filesSkippedBytes.With(l).Add(float64(skipped))
	rm.filesFailedBytes.With(l).Add(float64(failed))
}

// UpdateRestoreProgress updates restore "files_restored_bytes" metrics.
func (rm RestoreM) UpdateRestoreProgress(clusterID uuid.UUID, manifestPath, keyspace, table string, restored int64) {
	l := prometheus.Labels{
		"cluster":  clusterID.String(),
		"manifest": manifestPath,
		"keyspace": keyspace,
		"table":    table,
	}

	rm.filesRestoredBytes.With(l).Add(float64(restored))
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
