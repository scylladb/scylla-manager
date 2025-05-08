// Copyright (C) 2025 ScyllaDB

package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/scylladb/scylla-manager/backupspec"
	"github.com/scylladb/scylla-manager/v3/pkg/util/uuid"
)

type One2OneRestoreMetrics struct {
	remainingBytes  *prometheus.GaugeVec
	state           *prometheus.GaugeVec
	viewBuildStatus *prometheus.GaugeVec
}

func NewOne2OneRestoreMetrics() One2OneRestoreMetrics {
	g := gaugeVecCreator("one2onerestore")

	return One2OneRestoreMetrics{
		remainingBytes: g("Remaining bytes of backup to be restored yet.", "download_remaining_bytes",
			"source_cluster", "cluster", "snapshot_tag", "location", "dc", "node", "keyspace", "table"),
		state: g("Defines current state of the 1-1-restore process (downloading/loading/error/done).",
			"state", "source_cluster", "cluster", "location", "snapshot_tag", "host"),
		viewBuildStatus: g("Defines build status of recreated view.", "view_build_status", "cluster", "keyspace", "view"),
	}
}

// MustRegister shall be called to make the metrics visible by prometheus client.
func (m One2OneRestoreMetrics) MustRegister() One2OneRestoreMetrics {
	prometheus.MustRegister(m.all()...)
	return m
}

func (m One2OneRestoreMetrics) all() []prometheus.Collector {
	return []prometheus.Collector{
		m.remainingBytes,
		m.state,
		m.viewBuildStatus,
	}
}

// ResetClusterMetrics resets all 1-1-restore metrics labeled with the cluster.
func (m One2OneRestoreMetrics) ResetClusterMetrics(clusterID uuid.UUID) {
	for _, c := range m.all() {
		setGaugeVecMatching(c.(*prometheus.GaugeVec), unspecifiedValue, clusterMatcher(clusterID))
	}
}

// One2OneRestoreBytesLabels is a set of labels for restore metrics.
type One2OneRestoreBytesLabels struct {
	SourceClusterID, ClusterID string
	SnapshotTag                string
	Location                   string
	DC                         string
	Node                       string
	Keyspace                   string
	Table                      string
}

// SetDownloadRemainingBytes sets download_remaining_bytes metrics.
func (m One2OneRestoreMetrics) SetDownloadRemainingBytes(labels One2OneRestoreBytesLabels, remainingBytes float64) {
	l := prometheus.Labels{
		"source_cluster": labels.SourceClusterID,
		"cluster":        labels.ClusterID,
		"snapshot_tag":   labels.SnapshotTag,
		"location":       labels.Location,
		"dc":             labels.DC,
		"node":           labels.Node,
		"keyspace":       labels.Keyspace,
		"table":          labels.Table,
	}
	m.remainingBytes.With(l).Set(remainingBytes)
}

// One2OneRestoreState is the enum that defines how node is used during the 1-1-restore.
type One2OneRestoreState int

const (
	// One2OneRestoreStateDownloading means that node is downloading data from backup location.
	One2OneRestoreStateDownloading = iota
	// One2OneRestoreStateLoading means that node is calling load sstables (nodetool refresh).
	One2OneRestoreStateLoading
	// One2OneRestoreStateDone means that node ended downloading and loading data.
	One2OneRestoreStateDone
	// One2OneRestoreStateError means that node ended up with error.
	One2OneRestoreStateError
)

// SetOne2OneRestoreState sets 1-1-restore "state" metric.
func (m One2OneRestoreMetrics) SetOne2OneRestoreState(sourceClusterID, clusterID uuid.UUID, location backupspec.Location, snapshotTag, host string, state One2OneRestoreState) {
	l := prometheus.Labels{
		"source_cluster": sourceClusterID.String(),
		"cluster":        clusterID.String(),
		"location":       location.String(),
		"snapshot_tag":   snapshotTag,
		"host":           host,
	}
	m.state.With(l).Set(float64(state))
}

// SetViewBuildStatus sets 1-1-restore "view_build_status" metric.
func (m One2OneRestoreMetrics) SetViewBuildStatus(clusterID uuid.UUID, keyspace, view string, status ViewBuildStatus) {
	l := prometheus.Labels{
		"cluster":  clusterID.String(),
		"keyspace": keyspace,
		"view":     view,
	}
	m.viewBuildStatus.With(l).Set(float64(status))
}
