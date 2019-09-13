// Copyright (C) 2017 ScyllaDB

package restapi

import (
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/go-chi/chi/middleware"
	"github.com/scylladb/go-log"
)

func heartbeatMiddleware(endpoint string) func(http.Handler) http.Handler {
	return func(h http.Handler) http.Handler {
		fn := func(w http.ResponseWriter, r *http.Request) {
			if r.Method == "GET" && strings.EqualFold(r.URL.Path, endpoint) {
				w.WriteHeader(http.StatusNoContent)
				return
			}
			h.ServeHTTP(w, r)
		}
		return http.HandlerFunc(fn)
	}
}

func traceIDMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		r = r.WithContext(log.WithTraceID(r.Context()))
		next.ServeHTTP(w, r)
	})
}

// httpLogger implements a middleware.logFormatter for use with middleware.RequestLogger.
type httpLogger struct {
	l log.Logger
}

func (h httpLogger) NewLogEntry(r *http.Request) middleware.LogEntry {
	le := &httpLogEntry{
		req: r, l: h.l.With("method", r.Method, "uri", r.URL.RequestURI()),
	}
	return le
}

type httpLogEntry struct {
	req *http.Request
	l   log.Logger
	err error
}

func (e *httpLogEntry) Write(status, bytes int, elapsed time.Duration) {
	f := e.l.Debug
	if status >= 400 {
		f = e.l.Info
	}

	if e.err == nil {
		f(e.req.Context(), "HTTP",
			"status", status,
			"bytes", bytes,
			"duration", fmt.Sprintf("%dms", elapsed/1000000),
		)
	} else {
		f(e.req.Context(), "HTTP",
			"status", status,
			"bytes", bytes,
			"duration", fmt.Sprintf("%dms", elapsed/1000000),
			"error", e.err,
		)
	}
}

func (e *httpLogEntry) Panic(v interface{}, stack []byte) {
	e.l.Error(e.req.Context(), "Panic", "panic", v)
}

// AddError add error information to the log entry when it's printed.
func (e *httpLogEntry) AddError(err error) {
	e.err = err
}
