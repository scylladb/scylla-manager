// Copyright (C) 2023 ScyllaDB

package metrics

import (
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/scylladb/scylla-manager/backupspec"

	"github.com/scylladb/scylla-manager/v3/pkg/util/uuid"
)

type RestoreMetrics struct {
	batchSize        *prometheus.GaugeVec
	remainingBytes   *prometheus.GaugeVec
	state            *prometheus.GaugeVec
	restoredBytes    *prometheus.GaugeVec
	restoreDuration  *prometheus.GaugeVec
	downloadedBytes  *prometheus.GaugeVec
	downloadDuration *prometheus.GaugeVec
	streamedBytes    *prometheus.GaugeVec
	streamDuration   *prometheus.GaugeVec
	progress         *prometheus.GaugeVec
	viewBuildStatus  *prometheus.GaugeVec
}

func NewRestoreMetrics() RestoreMetrics {
	g := gaugeVecCreator("restore")

	return RestoreMetrics{
		batchSize: g("Cumulative size of the batches of files taken by the host to restore the data.", "batch_size", "cluster", "host"),
		remainingBytes: g("Remaining bytes of backup to be restored yet.", "remaining_bytes",
			"cluster", "snapshot_tag", "location", "dc", "node", "keyspace", "table"),
		state:            g("Defines current state of the restore process (idle/download/load/error).", "state", "cluster", "location", "snapshot_tag", "host"),
		restoredBytes:    g("Restored bytes", "restored_bytes", "cluster", "host"),
		restoreDuration:  g("Restore duration in ms", "restore_duration", "cluster", "host"),
		downloadedBytes:  g("Downloaded bytes", "downloaded_bytes", "cluster", "location", "host"),
		downloadDuration: g("Download duration in ms", "download_duration", "cluster", "location", "host"),
		streamedBytes:    g("Load&Streamed bytes", "streamed_bytes", "cluster", "host"),
		streamDuration:   g("Load&Stream duration in ms", "stream_duration", "cluster", "host"),
		progress:         g("Defines current progress of the restore process.", "progress", "cluster", "snapshot_tag"),
		viewBuildStatus:  g("Defines build status of recreated view.", "view_build_status", "cluster", "keyspace", "view"),
	}
}

// MustRegister shall be called to make the metrics visible by prometheus client.
func (m RestoreMetrics) MustRegister() RestoreMetrics {
	prometheus.MustRegister(m.all()...)
	return m
}

func (m RestoreMetrics) all() []prometheus.Collector {
	return []prometheus.Collector{
		m.batchSize,
		m.remainingBytes,
		m.state,
		m.restoredBytes,
		m.restoreDuration,
		m.downloadedBytes,
		m.downloadDuration,
		m.streamedBytes,
		m.streamDuration,
		m.progress,
		m.viewBuildStatus,
	}
}

// ResetClusterMetrics resets all restore metrics labeled with the cluster.
func (m RestoreMetrics) ResetClusterMetrics(clusterID uuid.UUID) {
	for _, c := range m.all() {
		setGaugeVecMatching(c.(*prometheus.GaugeVec), unspecifiedValue, clusterMatcher(clusterID))
	}
}

// IncreaseBatchSize updates restore "batch_size" metrics.
func (m RestoreMetrics) IncreaseBatchSize(clusterID uuid.UUID, host string, size int64) {
	l := prometheus.Labels{
		"cluster": clusterID.String(),
		"host":    host,
	}

	m.batchSize.With(l).Add(float64(size))
}

// DecreaseBatchSize updates restore "batch_size" metrics.
func (m RestoreMetrics) DecreaseBatchSize(clusterID uuid.UUID, host string, size int64) {
	l := prometheus.Labels{
		"cluster": clusterID.String(),
		"host":    host,
	}

	m.batchSize.With(l).Sub(float64(size))
}

// SetRemainingBytes sets restore "remaining_bytes" metric.
func (m RestoreMetrics) SetRemainingBytes(labels RestoreBytesLabels, remainingBytes int64) {
	l := prometheus.Labels{
		"cluster":      labels.ClusterID,
		"snapshot_tag": labels.SnapshotTag,
		"location":     labels.Location,
		"dc":           labels.DC,
		"node":         labels.Node,
		"keyspace":     labels.Keyspace,
		"table":        labels.Table,
	}
	m.remainingBytes.With(l).Set(float64(remainingBytes))
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
func (m RestoreMetrics) DecreaseRemainingBytes(labels RestoreBytesLabels, restoredBytes int64) {
	l := prometheus.Labels{
		"cluster":      labels.ClusterID,
		"snapshot_tag": labels.SnapshotTag,
		"location":     labels.Location,
		"dc":           labels.DC,
		"node":         labels.Node,
		"keyspace":     labels.Keyspace,
		"table":        labels.Table,
	}
	m.remainingBytes.With(l).Sub(float64(restoredBytes))
}

// RestoreProgressLabels is a set of labels for restore "progress" metric.
// RestoreProgressLabels does not contain DC and Node labels since we only care about global restore progress.
type RestoreProgressLabels struct {
	ClusterID   string
	SnapshotTag string
}

// SetProgress sets restore "progress" metric,
// progress should be a value between 0 and 100, that indicates global restore progress.
func (m RestoreMetrics) SetProgress(labels RestoreProgressLabels, progress float64) {
	l := prometheus.Labels{
		"cluster":      labels.ClusterID,
		"snapshot_tag": labels.SnapshotTag,
	}
	m.progress.With(l).Set(progress)
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
	// RestoreStateNativeRestore means that node is restoring with
	// native Scylla restore task.
	RestoreStateNativeRestore
)

// SetRestoreState sets restore "state" metric.
func (m RestoreMetrics) SetRestoreState(clusterID uuid.UUID, location backupspec.Location, snapshotTag, host string, state RestoreState) {
	l := prometheus.Labels{
		"cluster":      clusterID.String(),
		"location":     location.String(),
		"snapshot_tag": snapshotTag,
		"host":         host,
	}
	m.state.With(l).Set(float64(state))
}

// IncreaseRestoredBytes increases restore "restored_bytes" metric.
func (m RestoreMetrics) IncreaseRestoredBytes(clusterID uuid.UUID, host string, bytes int64) {
	l := prometheus.Labels{
		"cluster": clusterID.String(),
		"host":    host,
	}
	m.restoredBytes.With(l).Add(float64(bytes))
}

// IncreaseRestoreDuration increases restore "restore_duration" metric.
func (m RestoreMetrics) IncreaseRestoreDuration(clusterID uuid.UUID, host string, d time.Duration) {
	l := prometheus.Labels{
		"cluster": clusterID.String(),
		"host":    host,
	}
	m.restoreDuration.With(l).Add(float64(d.Milliseconds()))
}

// IncreaseRestoreDownloadedBytes increases restore "downloaded_bytes" metric.
func (m RestoreMetrics) IncreaseRestoreDownloadedBytes(clusterID uuid.UUID, location, host string, bytes int64) {
	l := prometheus.Labels{
		"cluster":  clusterID.String(),
		"location": location,
		"host":     host,
	}
	m.downloadedBytes.With(l).Add(float64(bytes))
}

// IncreaseRestoreDownloadDuration increases restore "download_duration" metric.
func (m RestoreMetrics) IncreaseRestoreDownloadDuration(clusterID uuid.UUID, location, host string, d time.Duration) {
	l := prometheus.Labels{
		"cluster":  clusterID.String(),
		"location": location,
		"host":     host,
	}
	m.downloadDuration.With(l).Add(float64(d.Milliseconds()))
}

// IncreaseRestoreStreamedBytes increases restore "streamed_bytes" metric.
func (m RestoreMetrics) IncreaseRestoreStreamedBytes(clusterID uuid.UUID, host string, bytes int64) {
	l := prometheus.Labels{
		"cluster": clusterID.String(),
		"host":    host,
	}
	m.streamedBytes.With(l).Add(float64(bytes))
}

// IncreaseRestoreStreamDuration increases restore "stream_duration" metric.
func (m RestoreMetrics) IncreaseRestoreStreamDuration(clusterID uuid.UUID, host string, d time.Duration) {
	l := prometheus.Labels{
		"cluster": clusterID.String(),
		"host":    host,
	}
	m.streamDuration.With(l).Add(float64(d.Milliseconds()))
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
func (m RestoreMetrics) SetViewBuildStatus(labels RestoreViewBuildStatusLabels, status ViewBuildStatus) {
	l := prometheus.Labels{
		"cluster":  labels.ClusterID,
		"keyspace": labels.Keyspace,
		"view":     labels.View,
	}
	m.viewBuildStatus.With(l).Set(float64(status))
}
