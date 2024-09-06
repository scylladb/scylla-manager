// Copyright (C) 2017 ScyllaDB

package restapi

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/scylladb/scylla-manager/v3/pkg"
)

func TestVersion(t *testing.T) {
	t.Parallel()

	h := Version()
	r := httptest.NewRequest(http.MethodGet, "/api/v1/version", nil)
	w := httptest.NewRecorder()
	h.ServeHTTP(w, r)

	var v version
	if err := json.NewDecoder(w.Result().Body).Decode(&v); err != nil {
		t.Fatal("json Decode() error", err)
	}

	if v.Version != pkg.Version() {
		t.Error("Version mismatch, should equal", pkg.Version(), "but equals", v.Version)
	}
}
