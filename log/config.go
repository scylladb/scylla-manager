// Copyright (C) 2017 ScyllaDB

package log

import (
	"log/syslog"
	"os"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

// NewDevelopment creates a new logger that writes DebugLevel and above
// logs to standard error in a human-friendly format.
func NewDevelopment() Logger {
	l, _ := zap.NewDevelopment()
	return Logger{base: l}
}

// NewProduction builds a sensible production Logger that writes DebugLevel and
// above logs to syslog as JSON.
func NewProduction(tag string) (Logger, error) {
	w, err := syslog.New(syslog.LOG_DAEMON, tag)
	if err != nil {
		return NopLogger, err
	}

	opts := []zap.Option{
		zap.ErrorOutput(os.Stderr),
		zap.AddStacktrace(zapcore.ErrorLevel),
	}

	cfg := zap.NewProductionEncoderConfig()
	// ignore level and time as they will be logged by syslog
	cfg.LevelKey = ""
	cfg.TimeKey = ""

	l := zap.New(NewSyslogCore(
		zapcore.NewJSONEncoder(cfg),
		w,
		zapcore.DebugLevel,
	), opts...)

	return Logger{base: l}, nil
}
