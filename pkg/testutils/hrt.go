// Copyright (C) 2017 ScyllaDB

package testutils

import (
	"net/http"
	"sync"
)

// HackableRoundTripper is a round tripper that allows for interceptor injection.
type HackableRoundTripper struct {
	inner       http.RoundTripper
	interceptor http.RoundTripper
	mu          sync.Mutex
}

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

// Interceptor returns the current interceptor.
func (h *HackableRoundTripper) Interceptor() http.RoundTripper {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.interceptor
}

// RoundTrip implements http.RoundTripper.
func (h *HackableRoundTripper) RoundTrip(req *http.Request) (resp *http.Response, err error) {
	if rt := h.Interceptor(); rt != nil {
		resp, err = rt.RoundTrip(req)
	}
	if resp == nil && err == nil {
		resp, err = h.inner.RoundTrip(req)
	}
	return
}
