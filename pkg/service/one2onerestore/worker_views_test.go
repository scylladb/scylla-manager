// Copyright (C) 2025 ScyllaDB

package one2onerestore

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"sync/atomic"
	"testing"
	"time"

	"github.com/scylladb/scylla-manager/v3/pkg/metrics"
	"github.com/scylladb/scylla-manager/v3/pkg/scyllaclient/scyllaclienttest"
)

func TestWaitForViewBuilding(t *testing.T) {
	testCases := []struct {
		name            string
		view            View
		handler         http.HandlerFunc
		contextProvider func() context.Context
		expectedCalls   int32
		expectedErr     bool
	}{
		{
			name: "Everything is fine",
			view: View{
				Keyspace: "ks",
				View:     "mv",
				Type:     MaterializedView,
			},
			handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Write([]byte(`[{"value":"SUCCESS"}]`))
			}),
			contextProvider: context.Background,
			expectedCalls:   1,
			expectedErr:     false,
		},
		{
			name: "Retries when not SUCCESS",
			view: View{
				Keyspace: "ks",
				View:     "mv",
				Type:     MaterializedView,
			},
			handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				// This magic header is set by testHandler.
				i := r.Header.Get("test-call-i")
				if i == "1" {
					w.Write([]byte(`[{"value":"STARTED"}]`))
					return
				}

				w.Write([]byte(`[{"value":"SUCCESS"}]`))
			}),
			contextProvider: context.Background,
			expectedCalls:   2,
			expectedErr:     false,
		},
		{
			name: "Exit on context cancel",
			view: View{
				Keyspace: "ks",
				View:     "mv",
				Type:     MaterializedView,
			},
			handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				time.Sleep(100 * time.Millisecond)
				w.Write([]byte(`[{"value":"SUCCESS"}]`))
			}),
			contextProvider: func() context.Context {
				ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
				_ = cancel
				return ctx
			},
			expectedErr:   true,
			expectedCalls: 1,
		},
		{
			name: "Exit on error",
			view: View{
				Keyspace: "ks",
				View:     "mv",
				Type:     MaterializedView,
			},
			handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(http.StatusTeapot)
				w.Write([]byte(`{"message": "wtf", "code": 418}`))
			}),
			contextProvider: context.Background,
			expectedErr:     true,
			expectedCalls:   1,
		},
		{
			name: "Exit on timeout",
			view: View{
				Keyspace: "ks",
				View:     "mv",
				Type:     MaterializedView,
			},
			handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(http.StatusRequestTimeout)
				w.Write([]byte(`{"message": "timeout", "code": 408}`))
			}),
			contextProvider: context.Background,
			expectedErr:     true,
			expectedCalls:   1,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			handler := &testHandler{Handler: tc.handler}
			ts := httptest.NewServer(handler)
			defer ts.Close()

			tsAddr, err := url.Parse(ts.URL)
			if err != nil {
				t.Fatalf("Unexpected err: %v", err)
			}
			w := &worker{
				client:  scyllaclienttest.MakeClient(t, tsAddr.Hostname(), tsAddr.Port()),
				metrics: metrics.NewOne2OneRestoreMetrics(),
			}
			err = w.waitForViewBuilding(tc.contextProvider(), tc.view, &RunViewProgress{})
			if err != nil && !tc.expectedErr {
				t.Fatalf("Unexpected err: %v", err)
			}
			if err == nil && tc.expectedErr {
				t.Fatalf("Expected err, but got nil")
			}
			if tc.expectedCalls != handler.calls.Load() {
				t.Fatalf("Expected %d calls, but got %d", tc.expectedCalls, handler.calls.Load())
			}
		})
	}
}

type testHandler struct {
	http.Handler
	// Keep track of how many times handler func has been called
	// so we can test retries policy.
	calls atomic.Int32
}

func (th *testHandler) ServeHTTP(w http.ResponseWriter, t *http.Request) {
	t.Header.Add("test-call-i", fmt.Sprint(th.calls.Add(1)))
	th.Handler.ServeHTTP(w, t)
}
