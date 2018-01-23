// Copyright (C) 2017 ScyllaDB

package mermaidtest

import (
	"net/http"
	"sync"
)

// The RoundTripperFunc type is an adapter to allow the use of ordinary
// functions as RoundTrippers. If f is a function with the appropriate
// signature, RountTripperFunc(f) is a RoundTripper that calls f.
type RoundTripperFunc func(req *http.Request) (*http.Response, error)

// RoundTrip implements the RoundTripper interface.
func (rt RoundTripperFunc) RoundTrip(r *http.Request) (*http.Response, error) {
	return rt(r)
}

// HackableRoundTripper is a round tripper that allows for interceptor injection.
type HackableRoundTripper struct {
	inner       http.RoundTripper
	interceptor http.RoundTripper
	mu          sync.Mutex
}

// NewHackableRoundTripper returns new HackableRoundTripper over the inner
// round tripper.
func NewHackableRoundTripper(inner http.RoundTripper) *HackableRoundTripper {
	return &HackableRoundTripper{
		inner: inner,
	}
}

// SetInterceptor sets an interceptor, requests are directed to the interceptor
// instead of the inner round tripper. If interceptor RoundTrip returns nil for
// both response and error the process falls back to inner round tripper.
func (h *HackableRoundTripper) SetInterceptor(rt http.RoundTripper) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.interceptor = rt
}

// RoundTrip implements http.RoundTripper.
func (h *HackableRoundTripper) RoundTrip(req *http.Request) (resp *http.Response, err error) {
	h.mu.Lock()
	rt := h.interceptor
	h.mu.Unlock()

	if rt != nil {
		resp, err = rt.RoundTrip(req)
	}
	if resp == nil && err == nil {
		resp, err = h.inner.RoundTrip(req)
	}
	return
}
