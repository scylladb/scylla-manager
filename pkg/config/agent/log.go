// Copyright (C) 2017 ScyllaDB

package agent

import (
	"github.com/scylladb/go-log"
	"github.com/scylladb/scylla-manager/pkg/config"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

func DefaultLogConfig() config.LogConfig {
	return config.LogConfig{
		Config: log.Config{
			Mode:  log.StderrMode,
			Level: zap.NewAtomicLevelAt(zapcore.InfoLevel),
			Sampling: &zap.SamplingConfig{
				Initial:    1,
				Thereafter: 100,
			},
		},
		Development: false,
	}
}

// MakeLogger creates application logger for the configuration.
func (c Config) MakeLogger() (log.Logger, error) {
	return config.MakeLogger(c.Logger)
}
