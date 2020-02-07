// Copyright (C) 2017 ScyllaDB

package httplog

import (
	"fmt"
	"net/http"
	"time"

	"github.com/go-chi/chi/middleware"
	"github.com/scylladb/go-log"
)

// TraceID adds trace ID to incoming request.
func TraceID(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		r = r.WithContext(log.WithTraceID(r.Context()))
		next.ServeHTTP(w, r)
	})
}

// RequestLogger logs requests and responses.
func RequestLogger(logger log.Logger) func(next http.Handler) http.Handler {
	return middleware.RequestLogger(&logFormatter{logger: logger})
}

// RequestLoggerSetRequestError adds error to request for rendering.
func RequestLoggerSetRequestError(r *http.Request, err error) {
	if le, _ := middleware.GetLogEntry(r).(*logEntry); le != nil {
		le.err = err
	}
}

type logFormatter struct {
	logger log.Logger
}

func (lf logFormatter) NewLogEntry(r *http.Request) middleware.LogEntry {
	return &logEntry{
		r: r,
		l: lf.logger,
	}
}

type logEntry struct {
	r   *http.Request
	l   log.Logger
	err error
}

func (le *logEntry) Write(status, bytes int, elapsed time.Duration) {
	f := []interface{}{
		"from", le.r.RemoteAddr,
		"method", le.r.Method,
		"uri", le.r.URL.RequestURI(),
		"status", status,
		"bytes", bytes,
		"duration", fmt.Sprintf("%dms", elapsed.Milliseconds()),
	}
	if le.err != nil {
		f = append(f, "error", le.err)
	}
	le.l.Info(le.r.Context(), "HTTP", f...)
}

func (le *logEntry) Panic(v interface{}, stack []byte) {
	le.l.Error(le.r.Context(), "Panic", "panic", v)
}
