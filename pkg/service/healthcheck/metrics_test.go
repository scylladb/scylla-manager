// Copyright (C) 2021 ScyllaDB

package healthcheck

import (
	"testing"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/scylladb/scylla-manager/v3/pkg/util/uuid"
)

// Removing of cluster metrics deadlocked when number of metrics exceeded buffered channel length
// used for metric collection.
// Issue #2843
func TestRemoveClusterMetricsWhenNumberOfMetricsExceedsDefaultChannelLength_2843(t *testing.T) {
	clusterID := uuid.MustRandom()
	metric := prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: "scylla_manager",
		Subsystem: "healthcheck",
		Name:      "status",
	}, []string{clusterKey, hostKey})
	for i := 0; i < 3*10*metricBufferSize; i++ {
		hl := prometheus.Labels{
			clusterKey: clusterID.String(),
			hostKey:    uuid.MustRandom().String(),
		}
		metric.With(hl).Set(1)
	}
	r := runner{metrics: &runnerMetrics{
		status: metric,
		rtt:    metric,
	}}

	r.removeMetricsForCluster(clusterID)
}
