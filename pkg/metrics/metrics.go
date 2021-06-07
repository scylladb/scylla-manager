// Copyright (C) 2017 ScyllaDB

package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
)

func gaugeVecCreator(subsystem string) func(help, name string, labels ...string) *prometheus.GaugeVec {
	return func(help, name string, labels ...string) *prometheus.GaugeVec {
		return prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: "scylla_manager",
			Subsystem: subsystem,
			Name:      name,
			Help:      help,
		}, labels)
	}
}

// setGaugeVecMatching sets metric instances with matching labels to the
// given value.
func setGaugeVecMatching(c *prometheus.GaugeVec, value float64, matcher func(*dto.Metric) bool) {
	var (
		data   dto.Metric
		labels []prometheus.Labels
	)

	for m := range collect(c) {
		if err := m.Write(&data); err != nil {
			continue
		}
		if matcher(&data) {
			labels = append(labels, makeLabels(data.Label))
		}
	}

	for _, l := range labels {
		m, err := c.GetMetricWith(l)
		if err != nil {
			panic(err)
		}
		m.Set(value)
	}
}
