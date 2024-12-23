// Copyright (C) 2017 ScyllaDB

package main

import (
	"encoding/json"
	"net"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/scylladb/go-log"
	"github.com/scylladb/scylla-manager/v3/pkg/config/agent"
)

func assertURLPath(t *testing.T, expected string) http.HandlerFunc {
	t.Helper()
	return func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != expected {
			t.Errorf("URL.Path=%s expected /foo", r.URL.Path)
		}
	}
}

func TestRcloneRouting(t *testing.T) {
	c := agent.Config{}
	rclone := assertURLPath(t, "/foo")

	h := newRouter(c, NewAgentMetrics(), rclone, nil, log.NewDevelopment())
	r := httptest.NewRequest(http.MethodGet, "/agent/rclone/foo", nil)
	w := httptest.NewRecorder()
	h.ServeHTTP(w, r)

	if w.Code != http.StatusOK {
		t.Errorf("Response Code=%d expected %d", w.Code, http.StatusOK)
	}
}

func TestProxyRouting(t *testing.T) {
	promStub := httptest.NewServer(assertURLPath(t, "/metrics"))
	defer promStub.Close()
	apiStub := httptest.NewServer(assertURLPath(t, "/storage_service/host_id"))
	defer apiStub.Close()

	promHost, promPort, _ := net.SplitHostPort(promStub.Listener.Addr().String())
	apiHost, apiPort, _ := net.SplitHostPort(apiStub.Listener.Addr().String())
	c := agent.Config{
		Scylla: agent.ScyllaConfig{
			PrometheusAddress: promHost,
			PrometheusPort:    promPort,
			APIAddress:        apiHost,
			APIPort:           apiPort,
		},
	}

	h := newRouter(c, NewAgentMetrics(), nil, nil, log.NewDevelopment())

	r := httptest.NewRequest(http.MethodGet, "/metrics", nil)
	w := httptest.NewRecorder()
	h.ServeHTTP(w, r)

	if w.Code != http.StatusOK {
		t.Errorf("Response Code=%d expected %d", w.Code, http.StatusOK)
	}

	r = httptest.NewRequest(http.MethodGet, "/storage_service/host_id", nil)
	w = httptest.NewRecorder()
	h.ServeHTTP(w, r)

	if w.Code != http.StatusOK {
		t.Errorf("Response Code=%d expected %d", w.Code, http.StatusOK)
	}
}

func TestCloudMetadataRouting(t *testing.T) {
	c := agent.Config{}
	rclone := http.HandlerFunc(func(http.ResponseWriter, *http.Request) {})
	cloudMeta := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte(`{"cloud_provider":"","instance_type":""}`))
	})

	h := newRouter(c, NewAgentMetrics(), rclone, cloudMeta, log.NewDevelopment())
	r := httptest.NewRequest(http.MethodGet, "/agent/cloud/metadata", nil)
	w := httptest.NewRecorder()
	h.ServeHTTP(w, r)

	if w.Code != http.StatusOK {
		t.Errorf("Response Code=%d expected %d", w.Code, http.StatusOK)
	}

	responseBody := map[string]string{}
	if err := json.NewDecoder(w.Result().Body).Decode(&responseBody); err != nil {
		t.Fatalf("decode body, unexpected err: %v", err)
	}

	cloudProvider, ok := responseBody["cloud_provider"]
	if !ok {
		t.Fatalf("`cloud_provider` field is expected")
	}
	if cloudProvider != "" {
		t.Fatalf("expects `cloud_provider` to be empty, got %s", cloudProvider)
	}

	instanceType, ok := responseBody["instance_type"]
	if !ok {
		t.Fatalf("`instance_type` field is expected")
	}
	if instanceType != "" {
		t.Fatalf("expects `instance_type` to be empty, got %s", instanceType)
	}
}
