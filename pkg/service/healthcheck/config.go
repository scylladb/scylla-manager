// Copyright (C) 2017 ScyllaDB

package healthcheck

import (
	"time"
)

// Config specifies the healthcheck service configuration.
type Config struct {
	// NodeInfoTTL specifies how long node info should be cached.
	NodeInfoTTL time.Duration `yaml:"node_info_ttl"`
	// MaxTimeout specifies ping timeout for all ping types (CQL, REST, Alternator).
	MaxTimeout time.Duration `yaml:"max_timeout"`
	// Deprecated: value is not used anymore
	Probes int `yaml:"probes"`
	// Deprecated: value is not used anymore
	RelativeTimeout time.Duration `yaml:"relative_timeout"`
}

func DefaultConfig() Config {
	return Config{
		MaxTimeout:  1 * time.Second,
		NodeInfoTTL: 5 * time.Minute,
	}
}
