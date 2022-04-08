// Copyright (C) 2017 ScyllaDB

package server

import (
	"github.com/scylladb/go-log"
	"github.com/scylladb/scylla-manager/v3/pkg/config"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

func DefaultLogConfig() config.LogConfig {
	return config.LogConfig{
		Config: log.Config{
			Mode:     log.StderrMode,
			Level:    zap.NewAtomicLevelAt(zapcore.InfoLevel),
			Encoding: log.JSONEncoding,
		},
		Development: false,
	}
}

// MakeLogger creates application logger for the configuration.
func (c Config) MakeLogger() (log.Logger, error) {
	return config.MakeLogger(c.Logger)
}
