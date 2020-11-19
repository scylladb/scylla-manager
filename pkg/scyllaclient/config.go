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
	// Port specifies the default Scylla Manager agent port.
	Port string
	// Transport scheme HTTP or HTTPS.
	Scheme string
	// AuthToken specifies the authentication token.
	AuthToken string
	// Timeout specifies time to complete a single request to Scylla REST API
	// possibly including opening a TCP connection.
	Timeout time.Duration
	// Backoff specifies parameters of exponential backoff used when requests
	// from Scylla Manager to Scylla Agent fail.
	Backoff BackoffConfig
	// InteractiveBackoff specifies backoff for interactive requests i.e.
	// originating from API / sctool.
	InteractiveBackoff BackoffConfig
	// PoolDecayDuration specifies size of time window to measure average
	// request time in Epsilon-Greedy host pool.
	PoolDecayDuration time.Duration

	// Transport allows for setting a custom round tripper to send HTTP
	// requests over not standard connections i.e. over SSH tunnel.
	Transport http.RoundTripper
}

// BackoffConfig specifies request exponential backoff parameters.
type BackoffConfig struct {
	WaitMin    time.Duration
	WaitMax    time.Duration
	MaxRetries uint64
	Multiplier float64
	Jitter     float64
}

// DefaultConfig returns a Config initialized with default values.
func DefaultConfig() Config {
	return Config{
		Port:    "10001",
		Scheme:  "https",
		Timeout: 15 * time.Second,
		Backoff: BackoffConfig{
			WaitMin:    1 * time.Second,
			WaitMax:    30 * time.Second,
			MaxRetries: 9,
			Multiplier: 2,
			Jitter:     0.2,
		},
		InteractiveBackoff: BackoffConfig{
			WaitMin:    time.Second,
			MaxRetries: 1,
		},
		PoolDecayDuration: 30 * time.Minute,
	}
}

// TestConfig is a convenience function equal to calling DefaultConfig and
// setting hosts and token manually.
func TestConfig(hosts []string, token string) Config {
	config := DefaultConfig()
	config.Hosts = hosts
	config.AuthToken = token

	config.Timeout = 5 * time.Second
	config.Backoff.MaxRetries = 2
	config.Backoff.WaitMin = 200 * time.Millisecond

	return config
}

// Validate checks if all the fields are properly set.
func (c Config) Validate() error {
	var err error
	if len(c.Hosts) == 0 {
		err = multierr.Append(err, errors.New("missing hosts"))
	}
	if c.Port == "" {
		err = multierr.Append(err, errors.New("missing port"))
	}

	return err
}
