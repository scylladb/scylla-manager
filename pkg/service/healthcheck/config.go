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
	// RelativeTimeout specifies ping timeout for all ping types (CQL, REST, Alternator)
	RelativeTimeout time.Duration `yaml:"relative_timeout"`
	// NodeInfoTTL specifies how long node info should be cached.
	NodeInfoTTL time.Duration `yaml:"node_info_ttl"`
	// Deprecated: value is not used anymore
	MaxTimeout time.Duration `yaml:"max_timeout"`
	// Deprecated: value is not used anymore
	Probes int `yaml:"probes"`
}

func DefaultConfig() Config {
	return Config{
		RelativeTimeout: 1 * time.Second,
		NodeInfoTTL:     5 * time.Minute,
	}
}
