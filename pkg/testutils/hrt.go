// Copyright (C) 2026 ScyllaDB

package testutils

import (
	"net/http"
	"sync"

	"github.com/scylladb/scylla-manager/v3/pkg/util/httpx"
)

// HackableRoundTripper is a round tripper that allows for interceptor injection.
type HackableRoundTripper struct {
	inner           http.RoundTripper
	interceptor     http.RoundTripper
	respInterceptor func(*http.Response, error) (*http.Response, error)
	mu              sync.Mutex
}

// NewHackableRoundTripper creates HackableRoundTripper with the inner round
// tripper wrapped with sequentialPermissionCheck. Requests handled fully by
// the interceptor are not affected.
func NewHackableRoundTripper(inner http.RoundTripper) *HackableRoundTripper {
	return &HackableRoundTripper{
		inner: sequentialPermissionCheck(inner),
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

// sequentialPermissionCheck is a default interceptor ensuring that permission
// check requests are executed sequentially. This is needed for our test env
// as parallel permission checks operating on objects with common prefix are
// not handled well by our MinIo container and result in test flakiness.
// This problem is not observed in production or mock GCS server.
func sequentialPermissionCheck(next http.RoundTripper) http.RoundTripper {
	const permissionCheckPath = "/agent/rclone/operations/check-permissions"
	permissionCheckSemaphore := make(chan struct{}, 1)
	permissionCheckSemaphore <- struct{}{}
	return httpx.RoundTripperFunc(func(req *http.Request) (*http.Response, error) {
		if req.URL.Path == permissionCheckPath {
			select {
			case <-permissionCheckSemaphore:
				defer func() {
					permissionCheckSemaphore <- struct{}{}
				}()
			case <-req.Context().Done():
				return nil, req.Context().Err()
			}
		}
		return next.RoundTrip(req)
	})
}
