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
	// Timeout specifies CQL ping timeout.
	Timeout time.Duration `yaml:"timeout"`
	// Timeout specifies encrypted CQL ping timeout.
	SSLTimeout time.Duration `yaml:"ssl_timeout"`
}

// DefaultConfig returns a Config initialized with default values.
func DefaultConfig() Config {
	return Config{
		Timeout:    250 * time.Millisecond,
		SSLTimeout: 750 * time.Millisecond,
	}
}
