// Copyright (C) 2017 ScyllaDB

package scyllaclient

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"time"

	"github.com/hailocab/go-hostpool"
	"github.com/scylladb/go-log"
	"github.com/scylladb/mermaid/internal/httputil"
	"github.com/scylladb/mermaid/internal/timeutc"
)

// openAPIRespDecorator adjusts Scylla REST API response so that it can be
// consumed by Open API.
func openAPIRespDecorator(parent http.RoundTripper) http.RoundTripper {
	return httputil.RoundTripperFunc(func(req *http.Request) (resp *http.Response, err error) {
		defer func() {
			if resp != nil {
				// Force JSON, Scylla returns "text/plain" that misleads the
				// unmarshaller and breaks processing.
				resp.Header.Set("Content-Type", "application/json")
			}
		}()
		return parent.RoundTrip(req)
	})
}

// hostPoolDispatcher sets request host from a pool.
func hostPoolDispatcher(parent http.RoundTripper, pool hostpool.HostPool) http.RoundTripper {
	return httputil.RoundTripperFunc(func(req *http.Request) (*http.Response, error) {
		ctx := req.Context()

		var (
			h   string
			hpr hostpool.HostPoolResponse
		)

		// get host from context
		h, ok := ctx.Value(ctxHost).(string)

		// get host from pool
		if !ok {
			hpr = pool.Get()
			h = hpr.Host()
		}

		// clone request
		r := cloneRequest(req)

		// set host
		r.Host = h
		r.URL.Host = h

		// RoundTrip shall not modify requests, here we modify it to fix error
		// messages see https://github.com/scylladb/mermaid/issues/266.
		// This is legit because we own the whole process. The modified request
		// is not being sent.
		req.Host = h
		req.URL.Host = h

		resp, err := parent.RoundTrip(r)

		// mark response
		if hpr != nil {
			hpr.Mark(err)
		}

		return resp, err
	})
}

// respLogger logs requests and responses.
func respLogger(parent http.RoundTripper, logger log.Logger) http.RoundTripper {
	return httputil.RoundTripperFunc(func(req *http.Request) (resp *http.Response, err error) {
		start := timeutc.Now()
		defer func() {
			if resp != nil {
				logger.Debug(req.Context(), "HTTP",
					"host", req.Host,
					"method", req.Method,
					"uri", req.URL.RequestURI(),
					"status", resp.StatusCode,
					"bytes", resp.ContentLength,
					"duration", fmt.Sprintf("%dms", timeutc.Since(start)/1000000),
				)
			}
		}()
		return parent.RoundTrip(req)
	})
}

// body defers context cancellation until response body is closed.
type body struct {
	io.ReadCloser
	cancel context.CancelFunc
}

func (b body) Close() error {
	defer b.cancel()
	return b.ReadCloser.Close()
}

// reqTimeout sets request context timeout for individual requests.
func reqTimeout(parent http.RoundTripper, timeout time.Duration) http.RoundTripper {
	return httputil.RoundTripperFunc(func(req *http.Request) (resp *http.Response, err error) {
		ctx, cancel := context.WithTimeout(req.Context(), timeout)
		defer func() {
			if resp != nil {
				resp.Body = body{resp.Body, cancel}
			}
		}()
		return parent.RoundTrip(req.WithContext(ctx))
	})
}

// cloneRequest creates a shallow copy of the request along with a deep copy
// of the Headers and URL.
func cloneRequest(req *http.Request) *http.Request {
	r := new(http.Request)

	// shallow clone
	*r = *req

	// deep copy headers
	r.Header = cloneHeader(req.Header)

	// deep copy URL
	r.URL = new(url.URL)
	*r.URL = *req.URL

	return r
}

// cloneHeader creates a deep copy of an http.Header.
func cloneHeader(in http.Header) http.Header {
	out := make(http.Header, len(in))
	for key, values := range in {
		newValues := make([]string, len(values))
		copy(newValues, values)
		out[key] = newValues
	}
	return out
}
