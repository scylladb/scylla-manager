// Copyright (C) 2017 ScyllaDB

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
		tableProgress: g("Tablet repair progress in percents (0-100).", "progress", "cluster", "keyspace", "table"),
	}
}

func (m TabletRepairMetrics) all() []prometheus.Collector {
	return []prometheus.Collector{
		m.tableProgress,
	}
}

// MustRegister shall be called to make the metrics visible by prometheus client.
func (m TabletRepairMetrics) MustRegister() TabletRepairMetrics {
	prometheus.MustRegister(m.all()...)
	return m
}

// ResetClusterMetrics resets all metrics labeled with the cluster.
func (m TabletRepairMetrics) ResetClusterMetrics(clusterID uuid.UUID) {
	for _, c := range m.all() {
		setGaugeVecMatching(c.(*prometheus.GaugeVec), unspecifiedValue, clusterMatcher(clusterID))
	}
}

// SetTableProgress updates "progress" metric.
func (m TabletRepairMetrics) SetTableProgress(clusterID uuid.UUID, keyspace, table string, progress float64) {
	l := prometheus.Labels{
		"cluster":  clusterID.String(),
		"keyspace": keyspace,
		"table":    table,
	}
	m.tableProgress.With(l).Set(progress)
}
