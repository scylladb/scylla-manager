// Copyright (C) 2024 ScyllaDB

package configcache

import "time"

// Config specifies the cache service configuration.
type Config struct {
	// UpdateFrequency specifies how often to call Scylla for its configuration.
	UpdateFrequency time.Duration `yaml:"update_frequency"`
}

func DefaultConfig() Config {
	return Config{
		UpdateFrequency: 5 * time.Minute,
	}
}
