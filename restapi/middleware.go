// Copyright (C) 2017 ScyllaDB

package restapi

import (
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/go-chi/chi/middleware"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	log "github.com/scylladb/golog"
)

func heartbeatMiddleware(endpoint string) func(http.Handler) http.Handler {
	f := func(h http.Handler) http.Handler {
		fn := func(w http.ResponseWriter, r *http.Request) {
			if r.Method == "GET" && strings.EqualFold(r.URL.Path, endpoint) {
				w.WriteHeader(http.StatusNoContent)
				return
			}
			h.ServeHTTP(w, r)
		}
		return http.HandlerFunc(fn)
	}
	return f
}

// prometheusMiddleware exposes promhttp.Handler as chi middleware. This is
// implemented this way to avoid interference with other middleware.
func prometheusMiddleware(endpoint string) func(http.Handler) http.Handler {
	f := func(next http.Handler) http.Handler {
		h := promhttp.Handler()

		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.Method == http.MethodGet && strings.EqualFold(r.URL.Path, endpoint) {
				h.ServeHTTP(w, r)
				return
			}
			next.ServeHTTP(w, r)
		})
	}

	return f
}

func traceIDMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		r = r.WithContext(log.WithTraceID(r.Context()))
		next.ServeHTTP(w, r)
	})
}

func recoverPanicsMiddleware(next http.Handler) http.Handler {
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
}

func (e *httpLogEntry) Write(status, bytes int, elapsed time.Duration) {
	e.l.Debug(e.req.Context(), "HTTP",
		"status", status,
		"bytes", bytes,
		"duration", fmt.Sprintf("%dms", elapsed/1000000),
	)
}

func (e *httpLogEntry) Panic(v interface{}, stack []byte) {
	e.l.Error(e.req.Context(), "Panic", "panic", v)
}

// AddFields appends additional log.Logger key-value pairs to the request
// log entry e.
func (e *httpLogEntry) AddFields(f ...interface{}) {
	e.l = e.l.With(f...)
}
