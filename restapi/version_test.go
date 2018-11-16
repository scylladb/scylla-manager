// Copyright (C) 2017 ScyllaDB

package restapi_test

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/scylladb/go-log"
	"github.com/scylladb/mermaid"
	"github.com/scylladb/mermaid/restapi"
)

func TestVersion(t *testing.T) {
	h := restapi.New(&restapi.Services{}, log.Logger{})
	r := httptest.NewRequest(http.MethodGet, "/api/v1/version", nil)
	w := httptest.NewRecorder()
	h.ServeHTTP(w, r)

	v := &restapi.Version{}
	err := json.NewDecoder(w.Result().Body).Decode(&v)
	if err != nil {
		t.Fatal(err)
	}

	if v.Version != mermaid.Version() {
		t.Error("Version mismatch, should equal", mermaid.Version(), "but equals", v.Version)
	}
}
