// Copyright (C) 2017 ScyllaDB

package cluster

import "github.com/prometheus/client_golang/prometheus"

var (
	sshOpenStreamsCount = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: "scylla_manager",
		Subsystem: "ssh",
		Name:      "open_streams_count",
		Help:      "Number of active (multiplexed) connections to Scylla node.",
	}, []string{"cluster", "host"})

	sshErrorsTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: "scylla_manager",
		Subsystem: "ssh",
		Name:      "errors_total",
		Help:      "Total number of SSH dial errors.",
	}, []string{"cluster", "host"})
)

func init() {
	prometheus.MustRegister(
		sshOpenStreamsCount,
		sshErrorsTotal,
	)
}
