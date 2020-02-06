// Copyright (C) 2017 ScyllaDB

package httpmw

import (
	"net"
	"net/http"
	"net/url"

	"github.com/hailocab/go-hostpool"
	"github.com/pkg/errors"
)

var errPoolServerError = errors.New("server error")

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
			switch {
			case err != nil:
				hpr.Mark(err)
			case resp.StatusCode >= 500:
				hpr.Mark(errPoolServerError)
			default:
				hpr.Mark(nil)
			}
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
