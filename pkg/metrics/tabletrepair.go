// Copyright (C) 2025 ScyllaDB

package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/scylladb/scylla-manager/v3/pkg/util/uuid"
)

// TabletRepairMetrics describes metrics of tablet repair task.
type TabletRepairMetrics struct {
	tableProgress *prometheus.GaugeVec
}

// NewTabletRepairMetrics creates new TabletRepairMetrics.
func NewTabletRepairMetrics() TabletRepairMetrics {
	g := gaugeVecCreator("tablet_repair")

	return TabletRepairMetrics{
		tableProgress: g("Tablet repair progress in percents (0-100).", "progress", "cluster", "task", "keyspace", "table"),
	}
}

func (m TabletRepairMetrics) all() []prometheus.Collector {
	return []prometheus.Collector{
		m.tableProgress,
	}
}

// MustRegister shall be called to make the metrics visible by prometheus client.
func (m TabletRepairMetrics) MustRegister() TabletRepairMetrics {
	return m.MustRegisterWith(prometheus.DefaultRegisterer)
}

// MustRegisterWith registers all tablet repair metrics with the given registerer.
func (m TabletRepairMetrics) MustRegisterWith(reg prometheus.Registerer) TabletRepairMetrics {
	reg.MustRegister(m.all()...)
	return m
}

// ResetClusterMetrics resets all metrics labeled with the cluster.
func (m TabletRepairMetrics) ResetClusterMetrics(clusterID uuid.UUID) {
	for _, c := range m.all() {
		setGaugeVecMatching(c.(*prometheus.GaugeVec), unspecifiedValue, clusterMatcher(clusterID))
	}
}

// DeleteTaskProgress removes the "progress" series of the task.
func (m TabletRepairMetrics) DeleteTaskProgress(taskID uuid.UUID) {
	m.tableProgress.DeletePartialMatch(prometheus.Labels{"task": taskID.String()})
}

// SetTableProgress updates "progress" metric.
func (m TabletRepairMetrics) SetTableProgress(clusterID, taskID uuid.UUID, keyspace, table string, progress float64) {
	l := prometheus.Labels{
		"cluster":  clusterID.String(),
		"task":     taskID.String(),
		"keyspace": keyspace,
		"table":    table,
	}
	m.tableProgress.With(l).Set(progress)
}
