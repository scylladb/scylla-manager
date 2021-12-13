// Copyright (C) 2017 ScyllaDB

package config

import (
	"github.com/scylladb/go-log"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

// LogConfig specifies logger configuration options.
type LogConfig struct {
	log.Config  `yaml:",inline"`
	Development bool `yaml:"development"`
}

func DefaultServerLogConfig() LogConfig {
	return LogConfig{
		Config: log.Config{
			Mode:  log.StderrMode,
			Level: zap.NewAtomicLevelAt(zapcore.InfoLevel),
		},
		Development: false,
	}
}

// MakeLogger creates application logger for the configuration.
func MakeLogger(c LogConfig) (log.Logger, error) {
	if c.Development {
		return log.NewDevelopmentWithLevel(c.Level.Level()), nil
	}
	return log.NewProduction(c.Config)
}
