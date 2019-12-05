// Copyright (C) 2017 ScyllaDB

package httpmw

import (
	"fmt"
	"net/http"
	"net/http/httputil"
	"time"

	"github.com/go-chi/chi/middleware"
	"github.com/pkg/errors"
	"github.com/scylladb/go-log"
	"github.com/scylladb/mermaid/pkg/util/timeutc"
)

// RequestTraceID adds trace ID to incoming request.
func RequestTraceID(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		r = r.WithContext(log.WithTraceID(r.Context()))
		next.ServeHTTP(w, r)
	})
}

// Logger logs requests and responses.
func Logger(next http.RoundTripper, logger log.Logger) http.RoundTripper {
	return RoundTripperFunc(func(req *http.Request) (resp *http.Response, err error) {
		start := timeutc.Now()
		resp, err = next.RoundTrip(req)
		logReqResp(logger, timeutc.Since(start), req, resp)
		return
	})
}

func logReqResp(logger log.Logger, elapsed time.Duration, req *http.Request, resp *http.Response) {
	f := []interface{}{
		"host", req.Host,
		"method", req.Method,
		"uri", req.URL.RequestURI(),
		"duration", fmt.Sprintf("%dms", elapsed.Milliseconds()),
	}
	logFn := logger.Debug
	if resp != nil {
		f = append(f,
			"status", resp.StatusCode,
			"bytes", resp.ContentLength,
		)

		// Dump body of failed requests, ignore 404s
		if c := resp.StatusCode; c >= 400 && c != http.StatusNotFound {
			if b, err := httputil.DumpResponse(resp, true); err != nil {
				f = append(f, "dump", errors.Wrap(err, "dump request"))
			} else {
				f = append(f, "dump", string(b))
			}
			logFn = logger.Info
		}
	}
	logFn(req.Context(), "HTTP", f...)
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
