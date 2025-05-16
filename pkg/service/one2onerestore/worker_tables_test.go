// Copyright (C) 2025 ScyllaDB

package one2onerestore

import (
	"context"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"

	"github.com/scylladb/scylla-manager/backupspec"
	"github.com/scylladb/scylla-manager/v3/pkg/metrics"
	"github.com/scylladb/scylla-manager/v3/pkg/scyllaclient/scyllaclienttest"
)

func TestRefreshNodeWorker(t *testing.T) {
	testCases := []struct {
		name          string
		inputCh       chan refreshNodeInput
		input         []refreshNodeInput
		handler       http.HandlerFunc
		expectedCalls int32
		expectedError bool
	}{
		{
			name:    "Happy path",
			inputCh: make(chan refreshNodeInput, 3),
			input: []refreshNodeInput{
				{
					ManifestInfo: &backupspec.ManifestInfo{},
					Progress:     &RunTableProgress{},
				},
				{
					ManifestInfo: &backupspec.ManifestInfo{},
					Progress:     &RunTableProgress{},
				},
				{
					ManifestInfo: &backupspec.ManifestInfo{},
					Progress:     &RunTableProgress{},
				},
			},
			handler: func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(http.StatusOK)
			},
			expectedCalls: 3,
			expectedError: false,
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

			for _, input := range tc.input {
				tc.inputCh <- input
			}
			close(tc.inputCh)
			// Start the refresh node worker
			err = w.refreshNodeWorker(context.Background(), tc.inputCh)()
			// Check the result
			if (err != nil) != tc.expectedError {
				t.Errorf("Expected error: %v, got: %v", tc.expectedError, err)
			}
			if tc.expectedCalls != handler.calls.Load() {
				t.Errorf("Expected %d calls, got: %d", tc.expectedCalls, handler.calls.Load())
			}
		})
	}
}
