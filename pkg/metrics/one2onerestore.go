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
	progress        *prometheus.GaugeVec
}

func NewOne2OneRestoreMetrics() One2OneRestoreMetrics {
	g := gaugeVecCreator("one2onerestore")

	return One2OneRestoreMetrics{
		remainingBytes: g("Remaining bytes of backup to be restored yet.", "download_remaining_bytes",
			"cluster", "snapshot_tag", "location", "dc", "node", "keyspace", "table"),
		state: g("Defines current state of the 1-1-restore process (downloading/loading/error/done).", "state",
			"cluster", "location", "snapshot_tag", "host", "worker"),
		viewBuildStatus: g("Defines build status of recreated view.", "view_build_status",
			"cluster", "keyspace", "view"),
		progress: g("Defines current progress of the 1-1-restore process.", "progress",
			"cluster", "snapshot_tag"),
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
		m.progress,
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
	ClusterID   string
	SnapshotTag string
	Location    string
	DC          string
	Node        string
	Keyspace    string
	Table       string
}

// SetDownloadRemainingBytes sets download_remaining_bytes metrics.
func (m One2OneRestoreMetrics) SetDownloadRemainingBytes(labels One2OneRestoreBytesLabels, remainingBytes float64) {
	l := prometheus.Labels{
		"cluster":      labels.ClusterID,
		"snapshot_tag": labels.SnapshotTag,
		"location":     labels.Location,
		"dc":           labels.DC,
		"node":         labels.Node,
		"keyspace":     labels.Keyspace,
		"table":        labels.Table,
	}
	m.remainingBytes.With(l).Set(remainingBytes)
}

// One2OneRestoreState is the enum that defines how node (worker on a node) is used during the 1-1-restore.
type One2OneRestoreState int

const (
	// One2OneRestoreStateIdle means that worker is idle.
	One2OneRestoreStateIdle = iota
	// One2OneRestoreStateDownloading means that worker is downloading data from backup location.
	One2OneRestoreStateDownloading
	// One2OneRestoreStateLoading means that worker is calling load sstables (nodetool refresh).
	One2OneRestoreStateLoading
	// One2OneRestoreStateDone means that worker ended downloading or loading all the data on a host.
	One2OneRestoreStateDone
	// One2OneRestoreStateError means that worker ended up with error.
	One2OneRestoreStateError
)

// SetOne2OneRestoreState sets 1-1-restore "state" metric.
func (m One2OneRestoreMetrics) SetOne2OneRestoreState(clusterID uuid.UUID, location backupspec.Location, snapshotTag, host, worker string, state One2OneRestoreState) {
	l := prometheus.Labels{
		"cluster":      clusterID.String(),
		"location":     location.String(),
		"snapshot_tag": snapshotTag,
		"host":         host,
		"worker":       worker,
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

// SetProgress sets 1-1-restore "progress" metric,
// progress should be a value between 0 and 100, that indicates global restore progress.
func (m One2OneRestoreMetrics) SetProgress(clusterID uuid.UUID, snapshotTag string, progress float64) {
	l := prometheus.Labels{
		"cluster":      clusterID.String(),
		"snapshot_tag": snapshotTag,
	}
	m.progress.With(l).Set(progress)
}
