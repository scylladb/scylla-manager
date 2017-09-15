// Copyright (C) 2017 ScyllaDB

package restapi

import (
	"net/http"
	"time"

	"github.com/go-chi/chi"
	"github.com/go-chi/chi/middleware"
	"github.com/go-chi/render"
	"github.com/scylladb/mermaid/api/repairapi"
	"github.com/scylladb/mermaid/log"
)

// New returns an http.Handler implementing mermaid v1 REST API.
func New(repairSvc repairapi.Service, logger log.Logger) http.Handler {
	r := chi.NewRouter()
	r.Use(middleware.RequestID)
	r.Use(middleware.RequestLogger(httpLogger{logger}))
	r.Use(render.SetContentType(render.ContentTypeJSON))

	r.Mount("/api/v1/cluster/{cluster_id}/repair/", repairapi.New(repairSvc))
	return r
}

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
