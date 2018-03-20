// Copyright (C) 2017 ScyllaDB

package log

import (
	"context"

	"github.com/prometheus/client_golang/prometheus"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

var (
	logInfoTotal = prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: "scylla_manager",
		Subsystem: "log",
		Name:      "info_total",
		Help:      "Total number of INFO messages.",
	})

	logErrorTotal = prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: "scylla_manager",
		Subsystem: "log",
		Name:      "error_total",
		Help:      "Total number of ERROR messages.",
	})
)

func init() {
	prometheus.MustRegister(
		logInfoTotal,
		logErrorTotal,
	)
}

// Logger logs messages.
type Logger struct {
	base *zap.Logger
}

// NewLogger creates a new logger backed by a zap.Logger.
func NewLogger(base *zap.Logger) Logger {
	return Logger{base: base}
}

// NopLogger doesn't log anything.
var NopLogger = Logger{}

// Named adds a new path segment to the logger's name. Segments are joined by
// periods. By default, Loggers are unnamed.
func (l Logger) Named(name string) Logger {
	if l.base == nil {
		return l
	}
	return Logger{base: l.base.Named(name)}
}

// With adds a variadic number of fields to the logging context.
func (l Logger) With(keyvals ...interface{}) Logger {
	if l.base == nil {
		return l
	}
	return Logger{base: l.base.With(l.zapify(context.Background(), keyvals)...)}
}

// Sync flushes any buffered log entries. Applications should take care to call
// Sync before exiting.
func (l Logger) Sync() error {
	if l.base == nil {
		return nil
	}
	return l.base.Sync()
}

// Debug logs a message with some additional context.
func (l Logger) Debug(ctx context.Context, msg string, keyvals ...interface{}) {
	l.log(ctx, zapcore.DebugLevel, msg, keyvals)
}

// Info logs a message with some additional context.
func (l Logger) Info(ctx context.Context, msg string, keyvals ...interface{}) {
	logInfoTotal.Inc()
	l.log(ctx, zapcore.InfoLevel, msg, keyvals)
}

// Error logs a message with some additional context.
func (l Logger) Error(ctx context.Context, msg string, keyvals ...interface{}) {
	logErrorTotal.Inc()
	l.log(ctx, zapcore.ErrorLevel, msg, keyvals)
}

// Fatal logs a message with some additional context, then calls os.Exit. The
// variadic key-value pairs are treated as they are in With.
func (l Logger) Fatal(ctx context.Context, msg string, keyvals ...interface{}) {
	l.log(ctx, zapcore.FatalLevel, msg, keyvals)
}

func (l Logger) log(ctx context.Context, lvl zapcore.Level, msg string, keyvals []interface{}) {
	if l.base == nil {
		return
	}
	if !l.base.Core().Enabled(lvl) {
		return
	}

	if ce := l.base.Check(lvl, msg); ce != nil {
		ce.Write(l.zapify(ctx, keyvals)...)
	}
}

func (l Logger) zapify(ctx context.Context, keyvals []interface{}) []zapcore.Field {
	if len(keyvals)%2 != 0 {
		l.base.DPanic("odd number of elements")
		return nil
	}

	var (
		extraFields int
		trace       *zapcore.Field
		ok          bool
	)

	if ctx != nil {
		trace, ok = ctx.Value(ctxTraceID).(*zapcore.Field)
		if ok {
			extraFields++
		}
	}

	if len(keyvals)+extraFields == 0 {
		return nil
	}

	fields := make([]zapcore.Field, 0, len(keyvals)/2+extraFields)
	for i := 0; i < len(keyvals); i += 2 {
		// Consume this value and the next, treating them as a key-value pair.
		key, val := keyvals[i], keyvals[i+1]
		if keyStr, ok := key.(string); !ok {
			l.base.DPanic("key not a string", zap.Any("key", key))
			break
		} else {
			fields = append(fields, zap.Any(keyStr, val))
		}
	}

	if trace != nil {
		fields = append(fields, *trace)
	}

	return fields
}
