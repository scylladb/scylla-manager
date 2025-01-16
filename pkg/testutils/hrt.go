// Copyright (C) 2017 ScyllaDB

package testutils

import (
	"net/http"
	"sync"
)

// HackableRoundTripper is a round tripper that allows for interceptor injection.
type HackableRoundTripper struct {
	inner           http.RoundTripper
	interceptor     http.RoundTripper
	respInterceptor func(*http.Response, error) (*http.Response, error)
	mu              sync.Mutex
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

// SetRespInterceptor sets a response interceptor which is called on responses returned by both
// interceptor and inner round tripper. If response interceptor returns nil for
// both response and error the process falls back to the original response and error.
func (h *HackableRoundTripper) SetRespInterceptor(ri func(*http.Response, error) (*http.Response, error)) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.respInterceptor = ri
}

// Interceptor returns the current interceptor.
func (h *HackableRoundTripper) Interceptor() http.RoundTripper {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.interceptor
}

// RespInterceptor returns the current respInterceptor.
func (h *HackableRoundTripper) RespInterceptor() func(*http.Response, error) (*http.Response, error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.respInterceptor
}

// RoundTrip implements http.RoundTripper.
func (h *HackableRoundTripper) RoundTrip(req *http.Request) (resp *http.Response, err error) {
	if rt := h.Interceptor(); rt != nil {
		resp, err = rt.RoundTrip(req)
	}
	if resp == nil && err == nil {
		resp, err = h.inner.RoundTrip(req)
	}
	if rn := h.RespInterceptor(); rn != nil {
		if respI, errI := rn(resp, err); respI != nil || errI != nil {
			resp, err = respI, errI
		}
	}
	return
}
