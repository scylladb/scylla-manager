// Copyright (C) 2017 ScyllaDB

package restapi

import (
	"fmt"
	"net/http"
	"time"

	"github.com/go-chi/chi/middleware"
	"github.com/go-chi/render"
	"github.com/scylladb/mermaid/log"
	"github.com/scylladb/mermaid/uuid"
)

func traceIDMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		r = r.WithContext(log.WithTraceID(r.Context()))
		next.ServeHTTP(w, r)
	})
}

func recoverPanics(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer func() {
			if rvr := recover(); rvr != nil {
				if le, _ := middleware.GetLogEntry(r).(*httpLogEntry); le != nil {
					le.Panic(rvr, nil)
				}
				httpErrorRender(w, r, rvr)
			}

		}()

		next.ServeHTTP(w, r)
	})
}

func requireClusterID(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		clusterID, err := reqClusterID(r)
		if err != nil || clusterID == uuid.Nil {
			render.Respond(w, r, httpErrBadRequest(err))
			return
		}
		r = r.WithContext(newClusterIDCtx(r.Context(), clusterID))
		next.ServeHTTP(w, r)
	})
}

// httpLogger implements a middleware.logFormatter for use with middleware.RequestLogger.
type httpLogger struct {
	l log.Logger
}

func (h httpLogger) NewLogEntry(r *http.Request) middleware.LogEntry {
	le := &httpLogEntry{
		req: r, l: h.l.With("method", r.Method, "url", r.URL),
	}
	return le
}

type httpLogEntry struct {
	req *http.Request
	l   log.Logger
}

func (e *httpLogEntry) Write(status, bytes int, elapsed time.Duration) {
	e.l.Debug(e.req.Context(), "HTTP",
		"status", status,
		"bytes", bytes,
		"duration", fmt.Sprintf("%dms", elapsed/1000000),
	)
}

func (e *httpLogEntry) Panic(v interface{}, stack []byte) {
	e.l.Error(e.req.Context(), "HTTP Panic", "panic", v)
}

// AddFields appends additional log.Logger key-value pairs to the request
// log entry e.
func (e *httpLogEntry) AddFields(f ...interface{}) {
	e.l = e.l.With(f...)
}
