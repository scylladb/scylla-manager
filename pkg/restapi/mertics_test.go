// Copyright (C) 2017 ScyllaDB

package restapi_test

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/scylladb/mermaid/pkg/restapi"
	"github.com/scylladb/mermaid/pkg/util/prom"
)

func TestMetrics(t *testing.T) {
	h := restapi.NewPrometheus(nil, &prom.MetricsWatcher{})
	r := httptest.NewRequest(http.MethodGet, "/metrics", nil)
	w := httptest.NewRecorder()
	h.ServeHTTP(w, r)

	if w.Code != http.StatusOK {
		t.Fatal(w.Code)
	}

	t.Log(w.Body)

	if w.Body.Len() < 100 {
		t.Error("invalid body")
	}
}
