// Copyright (C) 2017 ScyllaDB

package httpmw

import (
	"net"
	"net/http"
	"net/url"
	"time"

	"github.com/hailocab/go-hostpool"
	"github.com/scylladb/go-log"
	"github.com/scylladb/mermaid/pkg/util/retry"
	"github.com/scylladb/mermaid/pkg/util/retryablehttp"
)

// Retry retries request if needed.
func Retry(next http.RoundTripper, poolSize int, logger log.Logger) http.RoundTripper {
	// Retry policy while using a specified host.
	hostRetry := retryablehttp.NewTransport(next, logger)

	// Retry policy while using host pool, no backoff wait time, switch host as
	// fast as possible.
	poolRetry := retryablehttp.NewTransport(next, logger)
	// DefaultRetryPolicy uses special logic not to retry on broken connections,
	// here as we switch hosts we want to always retry.
	poolRetry.CheckRetry = func(req *http.Request, resp *http.Response, err error) (bool, error) {
		if req != nil {
			if err := req.Context().Err(); err != nil {
				return false, err
			}
		}
		if err != nil {
			return true, err
		}
		if c := resp.StatusCode; c == 0 || c == 401 || c == 403 || (c >= 500 && c != 501) {
			return true, nil
		}
		return false, nil
	}
	poolRetry.NewBackoff = func() retry.Backoff {
		return retry.WithMaxRetries(retry.BackoffFunc(func() time.Duration {
			return 0
		}), uint64(poolSize))
	}

	return RoundTripperFunc(func(req *http.Request) (resp *http.Response, err error) {
		if _, ok := req.Context().Value(ctxNoRetry).(bool); ok {
			return next.RoundTrip(req)
		}

		if _, ok := req.Context().Value(ctxHost).(string); ok {
			return hostRetry.RoundTrip(req)
		}

		return poolRetry.RoundTrip(req)
	})
}

// HostPool sets request host from a pool.
func HostPool(next http.RoundTripper, pool hostpool.HostPool, port string) http.RoundTripper {
	return RoundTripperFunc(func(req *http.Request) (*http.Response, error) {
		ctx := req.Context()

		var (
			h   string
			hpr hostpool.HostPoolResponse
		)

		// Get host from context
		h, ok := ctx.Value(ctxHost).(string)

		// Get host from pool
		if !ok {
			hpr = pool.Get()
			h = hpr.Host()
		}

		// Clone request
		r := cloneRequest(req)

		// Set host and port
		hp := net.JoinHostPort(h, port)
		r.Host = hp
		r.URL.Host = hp

		// RoundTrip shall not modify requests, here we modify it to fix error
		// messages see https://github.com/scylladb/mermaid/pkg/issues/266.
		// This is legit because we own the whole process. The modified request
		// is not being sent.
		req.Host = h
		req.URL.Host = h

		resp, err := next.RoundTrip(r)

		// Mark response
		if hpr != nil {
			hpr.Mark(err)
		}

		return resp, err
	})
}

// cloneRequest creates a shallow copy of the request along with a deep copy
// of the Headers and URL.
func cloneRequest(req *http.Request) *http.Request {
	r := new(http.Request)

	// Shallow clone
	*r = *req

	// Copy ctx
	r = r.WithContext(req.Context())

	// Deep copy headers
	r.Header = cloneHeader(req.Header)

	// Deep copy URL
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
