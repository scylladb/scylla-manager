// Copyright (C) 2017 ScyllaDB

package restapi

import (
	"net/http"
	"time"

	"github.com/go-chi/chi/middleware"
	"github.com/scylladb/mermaid/log"
)

type httpLogger struct {
	l log.Logger
}

func (h httpLogger) NewLogEntry(r *http.Request) middleware.LogEntry {
	return &httpLogEntry{req: r, l: h.l}
}

type httpLogEntry struct {
	req *http.Request
	l   log.Logger
}

func (e *httpLogEntry) Write(status, bytes int, elapsed time.Duration) {
	var fields = []interface{}{
		"method", e.req.Method, "URL", e.req.URL,
		"status", status, "bytes", bytes, "elapsed", elapsed,
	}
	if reqID := middleware.GetReqID(e.req.Context()); reqID != "" {
		fields = append(fields, "reqID", reqID)
	}
	e.l.Debug(e.req.Context(), "HTTP", fields...)
}

func (e *httpLogEntry) Panic(v interface{}, stack []byte) {
	var fields = []interface{}{"panic", v, "stacktrace", string(stack)}
	if reqID := middleware.GetReqID(e.req.Context()); reqID != "" {
		fields = append(fields, "reqID", reqID)
	}
	e.l.Error(e.req.Context(), "HTTP Panic", fields...)
}
