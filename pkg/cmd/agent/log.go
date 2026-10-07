// Copyright (C) 2023 ScyllaDB

package main

import (
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/go-chi/chi/v5"
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

func (le *logEntry) Write(status, bytes int, _ http.Header, elapsed time.Duration, _ any) {
	le.m.RecordStatusCode(le.r.Method, metricsPath(le.r), status)

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

func (le *logEntry) Panic(v any, stack []byte) {
	le.l.Error(le.r.Context(), "Panic", "panic", v, "stack", stack)
}

// metricsPath returns the request path to be used as a metrics label.
// Directly logging every path separately would result in distinct metric
// series for each request containing path param. For example, calls to
// /task_manager/wait_task/{task_id} would create series per task ID.
// To avoid that, we want to report all requests with the same path
// pattern in a single metic series. This is achieved by first registering
// all such known paths (scyllaAPIPathParam) in newRouter,
// so that here we can look up the pattern used to match this request
// and create metric label based on that.
func metricsPath(r *http.Request) string {
	// Check if router matched request against known path param paths,
	// so that their metric label can use pattern instead of the raw path.
	if p := chi.RouteContext(r.Context()).RoutePattern(); p != "" && !strings.HasSuffix(p, "/*") {
		return p
	}
	// For all other cases, label metric based on raw path
	return r.URL.EscapedPath()
}
