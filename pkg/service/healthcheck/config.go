// Copyright (C) 2017 ScyllaDB

package healthcheck

import (
	"crypto/tls"
	"time"
)

// Health check defaults.
var (
	DefaultCQLPort        = "9042"
	DefaultAlternatorPort = "8000"
	DefaultTLSConfig      = &tls.Config{
		InsecureSkipVerify: true,
	}
)

// Config specifies the healthcheck service configuration.
type Config struct {
	// RelativeTimeout specifies timeout over median ping duration in probes.
	// The number of probes kept in memory is specified by the probes parameter.
	// There are separate probes for different DCs and ping types
	// (CQL, REST, Alternator).
	RelativeTimeout time.Duration `yaml:"relative_timeout"`
	// MaxTimeout specifies maximum ping timeout, zero means no limit.
	MaxTimeout time.Duration `yaml:"max_timeout"`
	// Probes specifies how many probes are kept in memory for calculation.
	// For different ping types and datacenters there are different probe sets.
	Probes int `yaml:"probes"`
	// NodeInfoTTL specifies how long node info should be cached.
	NodeInfoTTL time.Duration `yaml:"node_info_ttl"`
}

func DefaultConfig() Config {
	return Config{
		RelativeTimeout: 50 * time.Millisecond,
		MaxTimeout:      30 * time.Second,
		Probes:          200,
		NodeInfoTTL:     5 * time.Minute,
	}
}
