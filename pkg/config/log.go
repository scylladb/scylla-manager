// Copyright (C) 2017 ScyllaDB

package config

import (
	"github.com/scylladb/go-log"
	"go.uber.org/zap/zapcore"
)

// LogConfig specifies logger configuration options.
type LogConfig struct {
	Mode        log.Mode      `yaml:"mode"`
	Level       zapcore.Level `yaml:"level"`
	Development bool          `yaml:"development"`
}

func DefaultLogConfig() LogConfig {
	return LogConfig{
		Mode:        log.StderrMode,
		Level:       zapcore.InfoLevel,
		Development: false,
	}
}
