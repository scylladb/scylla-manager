// Copyright (C) 2017 ScyllaDB

package healthcheck

import (
	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
)

const (
	clusterKey = "cluster"
	dcKey      = "dc"
	rackKey    = "rack"
	hostKey    = "host"

	metricBufferSize = 100
)

func labelNames() []string {
	return []string{clusterKey, dcKey, rackKey, hostKey}
}

type labels struct {
	cluster string
	dc      string
	rack    string
	host    string
}

func newLabels(cluster, dc, rack, host string) labels {
	return labels{
		cluster: cluster,
		dc:      dc,
		rack:    rack,
		host:    host,
	}
}

func (ls labels) promLabels() prometheus.Labels {
	return prometheus.Labels{
		clusterKey: ls.cluster,
		dcKey:      ls.dc,
		rackKey:    ls.rack,
		hostKey:    ls.host,
	}
}

var (
	cqlStatus = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: "scylla_manager",
		Subsystem: "healthcheck",
		Name:      "cql_status",
		Help:      "Host native port status. -2 stands for unavailable agent, -1 for unavailable Scylla and 1 for everything is fine.",
	}, labelNames())

	cqlRTT = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: "scylla_manager",
		Subsystem: "healthcheck",
		Name:      "cql_rtt_ms",
		Help:      "Host native port RTT.",
	}, labelNames())

	restStatus = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: "scylla_manager",
		Subsystem: "healthcheck",
		Name:      "rest_status",
		Help:      "Host REST status. -2 stands for unavailable agent, -1 for unavailable Scylla and 1 for everything is fine.",
	}, labelNames())

	restRTT = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: "scylla_manager",
		Subsystem: "healthcheck",
		Name:      "rest_rtt_ms",
		Help:      "Host REST RTT.",
	}, labelNames())

	alternatorStatus = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: "scylla_manager",
		Subsystem: "healthcheck",
		Name:      "alternator_status",
		Help:      "Host Alternator status. -2 stands for unavailable agent, -1 for unavailable Scylla and 1 for everything is fine.",
	}, labelNames())

	alternatorRTT = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: "scylla_manager",
		Subsystem: "healthcheck",
		Name:      "alternator_rtt_ms",
		Help:      "Host Alternator RTT.",
	}, labelNames())
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

func apply(metrics []prometheus.Metric, f func(ls labels, v float64)) {
	for _, m := range metrics {
		metric := &dto.Metric{}
		if err := m.Write(metric); err != nil {
			continue
		}

		var c, dc, r, h string
		for _, l := range metric.GetLabel() {
			if l.GetName() == clusterKey {
				c = l.GetValue()
			}
			if l.GetName() == dcKey {
				dc = l.GetValue()
			}
			if l.GetName() == rackKey {
				r = l.GetValue()
			}
			if l.GetName() == hostKey {
				h = l.GetValue()
			}
		}
		f(newLabels(c, dc, r, h), metric.GetGauge().GetValue())
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
