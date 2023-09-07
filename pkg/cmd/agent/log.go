// Copyright (C) 2023 ScyllaDB

package main

import (
	"fmt"
	"net/http"
	"time"

	"github.com/go-chi/chi/v5/middleware"
	"github.com/scylladb/go-log"
)

// RequestLogger populates metrics on all responses but logs only on errors.
func RequestLogger(logger log.Logger, metrics AgentMetrics) func(next http.Handler) http.Handler {
	return middleware.RequestLogger(&logFormatter{logger: logger, metrics: metrics})
}

type logFormatter struct {
	logger  log.Logger
	metrics AgentMetrics
}

func (lf logFormatter) NewLogEntry(r *http.Request) middleware.LogEntry {
	return &logEntry{
		r: r,
		l: lf.logger,
		m: lf.metrics,
	}
}

type logEntry struct {
	r *http.Request
	l log.Logger
	m AgentMetrics
}

func (le *logEntry) Write(status, bytes int, _ http.Header, elapsed time.Duration, _ interface{}) {
	le.m.RecordStatusCode(le.r.Method, le.r.URL.EscapedPath(), status)

	ctx := le.r.Context()
	uri := le.r.Method + " " + le.r.URL.RequestURI()
	f := []any{
		"from", le.r.RemoteAddr,
		"status", status,
		"bytes", bytes,
		"duration", fmt.Sprintf("%dms", elapsed.Milliseconds()),
	}

	if status < 400 {
		le.l.Debug(ctx, uri, f...)
	} else {
		le.l.Error(ctx, uri, f...)
	}
}

func (le *logEntry) Panic(v interface{}, stack []byte) {
	le.l.Error(le.r.Context(), "Panic", "panic", v, "stack", stack)
}
