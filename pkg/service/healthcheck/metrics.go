// Copyright (C) 2017 ScyllaDB

package healthcheck

import (
	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
)

const (
	clusterKey  = "cluster"
	hostKey     = "host"
	dcKey       = "dc"
	pingTypeKey = "ping_type"
	rackKey     = "rack"

	metricBufferSize = 100
)

var (
	cqlStatus = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: "scylla_manager",
		Subsystem: "healthcheck",
		Name:      "cql_status",
		Help:      "Host native port status. -2 stands for unavailable agent, -1 for unavailable Scylla and 1 for everything is fine.",
	}, []string{clusterKey, hostKey, dcKey, rackKey})

	cqlRTT = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: "scylla_manager",
		Subsystem: "healthcheck",
		Name:      "cql_rtt_ms",
		Help:      "Host native port RTT.",
	}, []string{clusterKey, hostKey, dcKey, rackKey})

	restStatus = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: "scylla_manager",
		Subsystem: "healthcheck",
		Name:      "rest_status",
		Help:      "Host REST status. -2 stands for unavailable agent, -1 for unavailable Scylla and 1 for everything is fine.",
	}, []string{clusterKey, hostKey, dcKey, rackKey})

	restRTT = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: "scylla_manager",
		Subsystem: "healthcheck",
		Name:      "rest_rtt_ms",
		Help:      "Host REST RTT.",
	}, []string{clusterKey, hostKey, dcKey, rackKey})

	alternatorStatus = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: "scylla_manager",
		Subsystem: "healthcheck",
		Name:      "alternator_status",
		Help:      "Host Alternator status. -2 stands for unavailable agent, -1 for unavailable Scylla and 1 for everything is fine.",
	}, []string{clusterKey, hostKey, dcKey, rackKey})

	alternatorRTT = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: "scylla_manager",
		Subsystem: "healthcheck",
		Name:      "alternator_rtt_ms",
		Help:      "Host Alternator RTT.",
	}, []string{clusterKey, hostKey, dcKey, rackKey})
)

func init() {
	prometheus.MustRegister(
		cqlStatus,
		cqlRTT,
		restStatus,
		restRTT,
		alternatorStatus,
		alternatorRTT,
	)
}

func apply(metrics []prometheus.Metric, f func(cluster, dc, host, pt string, v float64)) {
	for _, m := range metrics {
		metric := &dto.Metric{}
		if err := m.Write(metric); err != nil {
			continue
		}
		var c, dc, h, pt string

		for _, l := range metric.GetLabel() {
			if l.GetName() == clusterKey {
				c = l.GetValue()
			}
			if l.GetName() == hostKey {
				h = l.GetValue()
			}
			if l.GetName() == dcKey {
				dc = l.GetValue()
			}
			if l.GetName() == pingTypeKey {
				pt = l.GetValue()
			}
		}
		f(c, dc, h, pt, metric.GetGauge().GetValue())
	}
}

func collect(g *prometheus.GaugeVec) []prometheus.Metric {
	ch := make(chan prometheus.Metric, metricBufferSize)
	go func() {
		g.Collect(ch)
		close(ch)
	}()

	// Collect holds RLock during entire call.
	// Read the entire channel into an array to prevent passing the channel back
	// where processing particular items could access the same lock and create
	// a deadlock as the Collect function has nowhere to write to until the items are read.
	var metrics []prometheus.Metric
	for m := range ch {
		metrics = append(metrics, m)
	}

	return metrics
}
