// Copyright (C) 2017 ScyllaDB

package restapi

import (
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/pkg/errors"

	"github.com/go-chi/chi/middleware"
	"github.com/go-chi/render"
	"github.com/scylladb/go-log"
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

func traceIDMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		r = r.WithContext(log.WithTraceID(r.Context()))
		next.ServeHTTP(w, r)
	})
}

func recoverPanicsMiddleware(logger log.Logger) func(next http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			defer func() {
				if rvr := recover(); rvr != nil {
					rvrs := fmt.Sprintf("%+v", rvr)
					if le, _ := middleware.GetLogEntry(r).(*httpLogEntry); le != nil {
						le.Panic(rvr, nil)
					} else {
						logger.Error(r.Context(), rvrs)
					}
					httpErrorRender(w, r, errors.New(rvrs))
				}

			}()

			next.ServeHTTP(w, r)
		})
	}
}

func httpErrorRender(w http.ResponseWriter, r *http.Request, v interface{}) {
	if err, ok := v.(error); ok {
		httpErr, _ := v.(*httpError)
		if httpErr == nil {
			httpErr = &httpError{
				Cause:      err.Error(),
				StatusCode: http.StatusInternalServerError,
				Message:    "unexpected error, consult logs",
				TraceID:    log.TraceID(r.Context()),
			}
		}

		if le, _ := middleware.GetLogEntry(r).(*httpLogEntry); le != nil {
			le.AddError(err)
		}

		render.Status(r, httpErr.StatusCode)
		render.DefaultResponder(w, r, httpErr)
		return
	}

	render.DefaultResponder(w, r, v)
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

	switch {
	case status >= 500:
		f = e.l.Error
	case status >= 400:
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
