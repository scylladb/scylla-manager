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
	}, []string{clusterKey, dcKey})

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
	}, []string{clusterKey, dcKey})

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
	}, []string{clusterKey, dcKey})

	rttMean = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: "scylla_manager",
		Subsystem: "healthcheck",
		Name:      "rtt_mean_ms",
		Help:      "Statistical mean for the collection of recent runs",
	}, []string{clusterKey, dcKey, pingTypeKey})

	rttStandardDeviation = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: "scylla_manager",
		Subsystem: "healthcheck",
		Name:      "standard_deviation_ms",
		Help:      "Standard deviation for rtt duration over collection of recent runs",
	}, []string{clusterKey, dcKey, pingTypeKey})

	rttNoise = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: "scylla_manager",
		Subsystem: "healthcheck",
		Name:      "noise_ms",
		Help:      "How much to add to the mean to get timeout value",
	}, []string{clusterKey, dcKey, pingTypeKey})
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
		rttMean,
		rttStandardDeviation,
		rttNoise,
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
