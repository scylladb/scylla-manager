// Copyright (C) 2024 ScyllaDB

package cloudmeta

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/scylladb/go-log"
)

func TestAzureMetadata(t *testing.T) {
	testCases := []struct {
		name    string
		handler http.Handler

		expectedCalls int
		expectedErr   bool
		expectedMeta  InstanceMetadata
	}{
		{
			name: "when response is 200",
			handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				testCheckRequireParams(t, r)

				w.Write([]byte(`{"compute":{"vmSize":"Standard-A3"}}`))
			}),
			expectedCalls: 1,
			expectedErr:   false,
			expectedMeta: InstanceMetadata{
				CloudProvider: CloudProviderAzure,
				InstanceType:  "Standard-A3",
			},
		},
		{
			name: "when response is 404: not retryable",
			handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				testCheckRequireParams(t, r)

				w.WriteHeader(http.StatusNotFound)
				w.Write([]byte(`internal server error`))
			}),
			expectedCalls: 1,
			expectedErr:   true,
			expectedMeta:  InstanceMetadata{},
		},
		{
			name: "when response is 500: retryable",
			handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				testCheckRequireParams(t, r)

				w.WriteHeader(http.StatusInternalServerError)
				w.Write([]byte(`internal server error`))
			}),
			expectedCalls: 4,
			expectedErr:   true,
			expectedMeta:  InstanceMetadata{},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			handler := &testHandler{Handler: tc.handler}
			testSrv := httptest.NewServer(handler)
			defer testSrv.Close()

			azureMeta := newAzureMetadata(log.NewDevelopment())
			azureMeta.baseURL = testSrv.URL

			meta, err := azureMeta.Metadata(context.Background())
			if tc.expectedErr && err == nil {
				t.Fatalf("expected err: %v\n", err)
			}
			if !tc.expectedErr && err != nil {
				t.Fatalf("unexpected err: %v\n", err)
			}

			if tc.expectedCalls != handler.calls {
				t.Fatalf("unexected number of calls: %d != %d", handler.calls, tc.expectedCalls)
			}

			if meta.CloudProvider != tc.expectedMeta.CloudProvider {
				t.Fatalf("unexpected cloud provider: %s", meta.CloudProvider)
			}

			if meta.InstanceType != tc.expectedMeta.InstanceType {
				t.Fatalf("unexpected instance type: %s", meta.InstanceType)
			}
		})
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
	th.Handler.ServeHTTP(w, t)
}

func testCheckRequireParams(t *testing.T, r *http.Request) {
	t.Helper()
	metadataHeader := r.Header.Get("Metadata")
	if metadataHeader != "true" {
		t.Fatalf("Metadata: true header is required")
	}
	apiVersion := r.URL.Query().Get("api-version")
	if apiVersion != azureAPIVersion {
		t.Fatalf("unexpected ?api-version: %s", apiVersion)
	}
}
