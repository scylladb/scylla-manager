// Copyright (C) 2017 ScyllaDB

package scyllaclient

import (
	"net/http"
	"time"

	"github.com/pkg/errors"
	"go.uber.org/multierr"
)

// Config specifies the Client configuration.
type Config struct {
	// Hosts specifies all the cluster hosts that for a pool of hosts for the
	// client.
	Hosts []string
	// Transport allows for setting a custom round tripper to send HTTP requests
	// over not standard connections i.e. over SSH tunnel.
	Transport http.RoundTripper

	// Timeout specifies end-to-end time to complete Scylla REST API request
	// including retries.
	Timeout time.Duration
	// RequestTimeout specifies time to complete a single request to Scylla
	// REST API possibly including opening SSH tunneled connection.
	RequestTimeout time.Duration
	// PoolDecayDuration specifies size of time window to measure average
	// request time in Epsilon-Greedy host pool.
	PoolDecayDuration time.Duration

	// APIPort specifies the default Scylla API port.
	APIPort string
	// MetricsPort specifies the default Scylla Prometheus metrics endpoint port.
	MetricsPort string
}

// DefaultConfig returns a Config initialized with default values.
func DefaultConfig() Config {
	return Config{
		Timeout:           30 * time.Second,
		RequestTimeout:    5 * time.Second,
		PoolDecayDuration: 30 * time.Minute,

		APIPort:     "10000",
		MetricsPort: "9180",
	}
}

// Validate checks if all the fields are properly set.
func (c Config) Validate() error {
	var err error
	if len(c.Hosts) == 0 {
		err = multierr.Append(err, errors.New("missing hosts"))
	}
	if c.APIPort == "" {
		err = multierr.Append(err, errors.New("missing api_port"))
	}
	if c.MetricsPort == "" {
		err = multierr.Append(err, errors.New("missing metrics_port"))
	}

	return err
}
