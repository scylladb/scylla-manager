// Copyright (C) 2023 ScyllaDB

package main

import (
	"fmt"

	"github.com/prometheus/client_golang/prometheus"
)

type AgentMetrics struct {
	StatusCode *prometheus.CounterVec
}

func NewAgentMetrics() AgentMetrics {
	return AgentMetrics{
		StatusCode: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: "scylla_manager_agent",
			Subsystem: "http",
			Name:      "status_code",
			Help:      "Total count of http requests with response status code.",
		}, []string{"method", "path", "code"}),
	}
}

// MustRegister shall be called to make the metrics visible by prometheus client.
func (m AgentMetrics) MustRegister() AgentMetrics {
	prometheus.MustRegister(m.StatusCode)
	return m
}

// RecordStatusCode increases counter of "status_code" metric.
func (m AgentMetrics) RecordStatusCode(method, path string, status int) {
	m.StatusCode.WithLabelValues(method, path, fmt.Sprint(status)).Inc()
}
