// Copyright (C) 2017 ScyllaDB

package main

import (
	"encoding/json"
	"maps"
	"net"
	"net/http"
	"net/http/httptest"
	"strconv"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
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

func TestMetricsPath(t *testing.T) {
	ok := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	})
	apiStub := httptest.NewServer(ok)
	defer apiStub.Close()
	apiHost, apiPort, _ := net.SplitHostPort(apiStub.Listener.Addr().String())
	c := agent.Config{
		Scylla: agent.ScyllaConfig{
			APIAddress: apiHost,
			APIPort:    apiPort,
		},
	}

	const taskID = "9b8c3d1e-1a2b-4c3d-8e9f-0a1b2c3d4e5f"
	testCases := []struct {
		method string
		path   string
		label  string
		code   int
	}{
		{http.MethodGet, "/task_manager/wait_task/" + taskID, "/task_manager/wait_task/{task_id}", http.StatusOK},
		{http.MethodGet, "/task_manager/task_status/" + taskID, "/task_manager/task_status/{task_id}", http.StatusOK},
		{http.MethodPost, "/task_manager/abort_task/" + taskID, "/task_manager/abort_task/{task_id}", http.StatusOK},
		{http.MethodPost, "/storage_service/repair_async/ks1", "/storage_service/repair_async/{keyspace}", http.StatusOK},
		{http.MethodPost, "/storage_service/keyspace_flush/ks1", "/storage_service/keyspace_flush/{keyspace}", http.StatusOK},
		{http.MethodPost, "/storage_service/sstables/ks1", "/storage_service/sstables/{keyspace}", http.StatusOK},
		{http.MethodGet, "/storage_service/describe_ring/ks1", "/storage_service/describe_ring/{keyspace}", http.StatusOK},
		{http.MethodGet, "/storage_service/view_build_statuses/ks1/view1", "/storage_service/view_build_statuses/{keyspace}/{view}", http.StatusOK},
		{http.MethodGet, "/storage_service/tokens/192.168.100.11", "/storage_service/tokens/{endpoint}", http.StatusOK},
		{http.MethodGet, "/column_family/autocompaction/ks1:tab1", "/column_family/autocompaction/{name}", http.StatusOK},
		{http.MethodPost, "/column_family/autocompaction/ks1:tab1", "/column_family/autocompaction/{name}", http.StatusOK},
		{http.MethodDelete, "/column_family/autocompaction/ks1:tab1", "/column_family/autocompaction/{name}", http.StatusOK},
		{http.MethodGet, "/column_family/metrics/total_disk_space_used/ks1:tab1", "/column_family/metrics/total_disk_space_used/{name}", http.StatusOK},
		// Fixed paths are recorded as they are
		{http.MethodGet, "/storage_service/host_id", "/storage_service/host_id", http.StatusOK},
		{http.MethodPost, "/agent/rclone/operations/list", "/agent/rclone/operations/list", http.StatusOK},
		{http.MethodGet, "/ping", "/ping", http.StatusNoContent},
	}

	for _, tc := range testCases {
		t.Run(tc.method+" "+tc.path, func(t *testing.T) {
			m := NewAgentMetrics()
			h := newRouter(c, m, ok, nil, log.NewDevelopment())
			r := httptest.NewRequest(tc.method, tc.path, nil)
			w := httptest.NewRecorder()
			h.ServeHTTP(w, r)

			if w.Code != tc.code {
				t.Fatalf("Response Code=%d expected %d", w.Code, tc.code)
			}
			series := collectStatusCodeSeries(t, m)
			if len(series) != 1 {
				t.Fatalf("Recorded %d series, expected 1: %v", len(series), series)
			}
			expected := map[string]string{
				"method": tc.method,
				"path":   tc.label,
				"code":   strconv.Itoa(tc.code),
			}
			if got := series[0].labels; !maps.Equal(got, expected) {
				t.Fatalf("Recorded labels %v, expected %v", got, expected)
			}
			if got := series[0].value; got != 1 {
				t.Fatalf("Recorded value %v, expected 1", got)
			}
		})
	}
}

type statusCodeSeries struct {
	labels map[string]string
	value  float64
}

// collectStatusCodeSeries returns all series recorded in the status code metric.
func collectStatusCodeSeries(t *testing.T, m AgentMetrics) []statusCodeSeries {
	t.Helper()

	ch := make(chan prometheus.Metric)
	go func() {
		m.StatusCode.Collect(ch)
		close(ch)
	}()

	var out []statusCodeSeries
	for metric := range ch {
		var d dto.Metric
		if err := metric.Write(&d); err != nil {
			t.Fatal(err)
		}
		labels := make(map[string]string)
		for _, l := range d.GetLabel() {
			labels[l.GetName()] = l.GetValue()
		}
		out = append(out, statusCodeSeries{labels: labels, value: d.GetCounter().GetValue()})
	}
	return out
}
