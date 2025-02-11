// Copyright (C) 2025 ScyllaDB

package one2onerestore

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"
	"time"

	"github.com/scylladb/scylla-manager/v3/pkg/scyllaclient/scyllaclienttest"
	"github.com/scylladb/scylla-manager/v3/pkg/service/backup/backupspec"
)

func TestWorkerRefreshNode(t *testing.T) {
	testCases := []struct {
		name            string
		handler         http.Handler
		contextProvider func() context.Context

		expectedCalls int
		expectedErr   bool
	}{
		{
			name: "Everything works",
			handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Write([]byte("OK"))
			}),
			contextProvider: context.Background,
			expectedCalls:   1,
		},
		{
			name: "Retries when Already in progress",
			handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				// This magic header is set by testHandler.
				i := r.Header.Get("test-call-i")

				if i == "1" {
					w.WriteHeader(http.StatusInternalServerError)
					w.Write([]byte(`{"message": "Already loading SSTables", "code": 500}`))
					return
				}
				w.Write([]byte("OK"))

			}),
			contextProvider: context.Background,
			expectedCalls:   2,
		},
		{
			name: "Retries when timeout",
			handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				// This magic header is set by testHandler.
				i := r.Header.Get("test-call-i")

				if i == "1" {
					w.WriteHeader(http.StatusRequestTimeout)
					w.Write([]byte(`{"message": "timeout", "code": 408}`))
					return
				}
				w.Write([]byte("OK"))

			}),
			contextProvider: context.Background,
			expectedCalls:   2,
		},
		{
			name: "Exit on ctx.Err",
			handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				time.Sleep(100 * time.Millisecond)
				w.Write([]byte("OK"))

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
			name: "Exit on unexpected err",
			handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(http.StatusTeapot)
				w.Write([]byte(`{"message": "wtf", "code": 418}`))
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
			requireNoError(t, err)

			w := &worker{
				client: scyllaclienttest.MakeClient(t, tsAddr.Hostname(), tsAddr.Port()),
			}

			const repeatInterval = 1 * time.Second
			err = w.refreshNode(tc.contextProvider(), backupspec.FilesMeta{}, Host{Addr: tsAddr.Hostname()}, repeatInterval)
			if !tc.expectedErr {
				requireNoError(t, err)
			}
			if tc.expectedErr {
				requireError(t, err)
			}
			requireTrue(t,
				tc.expectedCalls == handler.calls,
				fmt.Sprintf("Expected %d calls, but got %d", tc.expectedCalls, handler.calls))
		})
	}
}

func requireNoError(t *testing.T, err error) {
	t.Helper()
	if err != nil {
		t.Fatalf("Unexpected err: %v", err)
	}
}

func requireError(t *testing.T, err error) {
	t.Helper()
	if err == nil {
		t.Fatalf("Expected err, but got nil")
	}
}

func requireTrue(t *testing.T, b bool, msg string) {
	t.Helper()
	if !b {
		t.Fatal(msg)
	}
}

type testHandler struct {
	http.Handler
	// Keep track of how many times handler func has been called
	// so we can test retries policy.
	calls int
}

func (th *testHandler) ServeHTTP(w http.ResponseWriter, t *http.Request) {
	th.calls++
	t.Header.Add("test-call-i", fmt.Sprint(th.calls))
	th.Handler.ServeHTTP(w, t)
}
