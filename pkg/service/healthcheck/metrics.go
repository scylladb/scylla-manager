// Copyright (C) 2017 ScyllaDB

package healthcheck

import (
	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
)

const (
	clusterKey = "cluster"
	hostKey    = "host"

	metricBufferSize = 100
)

var (
	cqlStatus = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: "scylla_manager",
		Subsystem: "healthcheck",
		Name:      "cql_status",
		Help:      "Host native port status",
	}, []string{clusterKey, hostKey})

	cqlRTT = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: "scylla_manager",
		Subsystem: "healthcheck",
		Name:      "cql_rtt_ms",
		Help:      "Host native port RTT",
	}, []string{clusterKey, hostKey})

	cqlTimeout = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: "scylla_manager",
		Subsystem: "healthcheck",
		Name:      "cql_timeout_ms",
		Help:      "Host CQL Timeout",
	}, []string{clusterKey, hostKey})

	restStatus = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: "scylla_manager",
		Subsystem: "healthcheck",
		Name:      "rest_status",
		Help:      "Host REST status",
	}, []string{clusterKey, hostKey})

	restRTT = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: "scylla_manager",
		Subsystem: "healthcheck",
		Name:      "rest_rtt_ms",
		Help:      "Host REST RTT",
	}, []string{clusterKey, hostKey})

	restTimeout = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: "scylla_manager",
		Subsystem: "healthcheck",
		Name:      "rest_timeout_ms",
		Help:      "Host REST Timeout",
	}, []string{clusterKey, hostKey})

	alternatorStatus = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: "scylla_manager",
		Subsystem: "healthcheck",
		Name:      "alternator_status",
		Help:      "Host Alternator status",
	}, []string{clusterKey, hostKey})

	alternatorRTT = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: "scylla_manager",
		Subsystem: "healthcheck",
		Name:      "alternator_rtt_ms",
		Help:      "Host Alternator RTT",
	}, []string{clusterKey, hostKey})

	alternatorTimeout = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: "scylla_manager",
		Subsystem: "healthcheck",
		Name:      "alternator_timeout_ms",
		Help:      "Host Alternator Timeout",
	}, []string{clusterKey, hostKey})
)

func init() {
	prometheus.MustRegister(
		cqlStatus,
		cqlRTT,
		cqlTimeout,
		restStatus,
		restRTT,
		restTimeout,
		alternatorStatus,
		alternatorRTT,
		alternatorTimeout,
	)
}

func apply(ch <-chan prometheus.Metric, f func(cluster, host string, v float64)) {
	for m := range ch {
		metric := &dto.Metric{}
		if err := m.Write(metric); err != nil {
			continue
		}
		var c, h string

		for _, l := range metric.GetLabel() {
			if l.GetName() == clusterKey {
				c = l.GetValue()
			}
			if l.GetName() == hostKey {
				h = l.GetValue()
			}
		}
		f(c, h, metric.GetGauge().GetValue())
	}
}

func collect(g *prometheus.GaugeVec) chan prometheus.Metric {
	ch := make(chan prometheus.Metric, metricBufferSize)
	go func() {
		g.Collect(ch)
		close(ch)
	}()
	return ch
}
