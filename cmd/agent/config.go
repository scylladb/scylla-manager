// Copyright (C) 2017 ScyllaDB

package main

// config specifies the agent configuration.
type config struct {
	// ScyllaAPIPort specifies the default Scylla API port.
	ScyllaAPIPort string
	// ScyllaMetricsPort specifies the default Scylla Prometheus metrics endpoint port.
	ScyllaMetricsPort string
}

// defaultConfig returns a config initialized with default values.
func defaultConfig() config {
	return config{
		ScyllaAPIPort:     "10000",
		ScyllaMetricsPort: "9180",
	}
}
