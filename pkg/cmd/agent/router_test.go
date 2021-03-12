// Copyright (C) 2017 ScyllaDB

package main

import (
	"net"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/scylladb/go-log"
	"github.com/scylladb/scylla-manager/pkg/config"
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
	c := config.AgentConfig{}
	rclone := assertURLPath(t, "/foo")

	h := newRouter(c, rclone, log.NewDevelopment())
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
	c := config.AgentConfig{
		Scylla: config.ScyllaConfig{
			PrometheusAddress: promHost,
			PrometheusPort:    promPort,
			APIAddress:        apiHost,
			APIPort:           apiPort,
		},
	}

	h := newRouter(c, nil, log.NewDevelopment())

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
