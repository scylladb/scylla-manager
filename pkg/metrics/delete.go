// Copyright (C) 2017 ScyllaDB

package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
)

// CollectorDeleter extends prometheus.Collector with Delete.
type CollectorDeleter interface {
	prometheus.Collector
	Delete(labels prometheus.Labels) bool
}

// DeleteMatching removes metric instances with matching labels.
func DeleteMatching(c CollectorDeleter, matcher func(*dto.Metric) bool) {
	var data dto.Metric

	for m := range collect(c) {
		if err := m.Write(&data); err != nil {
			continue
		}
		if matcher(&data) {
			defer c.Delete(makeLabels(data.Label)) // nolint: staticcheck
		}
	}
}

func collect(c prometheus.Collector) chan prometheus.Metric {
	ch := make(chan prometheus.Metric)
	go func() {
		c.Collect(ch)
		close(ch)
	}()
	return ch
}

func makeLabels(pairs []*dto.LabelPair) prometheus.Labels {
	labels := make(prometheus.Labels)

	for _, kv := range pairs {
		if kv != nil {
			labels[kv.GetName()] = kv.GetValue()
		}
	}

	return labels
}
