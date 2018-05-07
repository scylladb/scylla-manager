// Copyright (C) 2017 ScyllaDB

package scyllaclient

import (
	"fmt"
	"net/http"
	"net/url"

	"github.com/hailocab/go-hostpool"
	log "github.com/scylladb/golog"
	"github.com/scylladb/mermaid/timeutc"
)

// transport is an http.RoundTriper that updates request host from context and
// invokes parent RoundTriper.
type transport struct {
	parent http.RoundTripper
	pool   hostpool.HostPool
	logger log.Logger
}

func (t transport) RoundTrip(req *http.Request) (*http.Response, error) {
	ctx := req.Context()

	var (
		h   string
		hpr hostpool.HostPoolResponse
	)

	// get host from context
	h, ok := ctx.Value(ctxHost).(string)

	// get host from pool
	if !ok {
		hpr = t.pool.Get()
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

	start := timeutc.Now()
	resp, err := t.parent.RoundTrip(r)
	if resp != nil {
		t.logger.Debug(ctx, "HTTP",
			"host", h,
			"method", r.Method,
			"uri", r.URL.RequestURI(),
			"status", resp.StatusCode,
			"bytes", resp.ContentLength,
			"duration", fmt.Sprintf("%dms", timeutc.Since(start)/1000000),
		)
		fixResponse(resp)
	}

	// mark response
	if hpr != nil {
		hpr.Mark(err)
	}

	return resp, err
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

// fixResponse fixes different Scylla API bugs...
func fixResponse(resp *http.Response) {
	// force JSON, Scylla returns "text/plain" that misleads the
	// unmarshaller and breaks processing.
	resp.Header.Set("Content-Type", "application/json")
}
