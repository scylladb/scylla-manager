// Copyright (C) 2017 ScyllaDB

package config

import (
	"github.com/scylladb/go-log"
)

// LogConfig specifies logger configuration options.
type LogConfig struct {
	log.Config  `yaml:",inline"`
	Development bool `yaml:"development"`
}

// MakeLogger creates application logger for the configuration.
func MakeLogger(c LogConfig) (log.Logger, error) {
	if c.Development {
		return log.NewDevelopmentWithLevel(c.Level.Level()), nil
	}
	return log.NewProduction(c.Config)
}
