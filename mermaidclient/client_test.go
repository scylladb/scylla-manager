// Copyright (C) 2017 ScyllaDB

package mermaidclient

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/go-openapi/runtime"
	"github.com/google/go-cmp/cmp"
	"github.com/scylladb/mermaid/uuid"
)

func TestClientClusterUUID(t *testing.T) {
	t.Parallel()

	u := uuid.MustRandom()
	c, err := NewClient("", u.String())
	if err != nil {
		t.Fatal(err)
	}

	v, err := c.clusterUUID()
	if err != nil {
		t.Fatal(err)
	}
	if v != u.String() {
		t.Fatal(v, u)
	}
}

func TestExtractUUIDFromLocation(t *testing.T) {
	t.Parallel()

	u0 := uuid.MustRandom()
	u1, err := extractUUIDFromLocation("http://bla/bla/" + u0.String() + "?param=true")
	if err != nil {
		t.Fatal(err)
	}
	if u1 != u0 {
		t.Fatal(u1, u0)
	}
}

func TestClientError(t *testing.T) {
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "{\"message\": \"bla\"}", 500)
	}))
	defer s.Close()

	c, err := NewClient(s.URL, uuid.MustRandom().String())
	if err != nil {
		t.Fatal(err)
	}

	_, err = c.ListRepairUnits(context.Background())
	if err == nil {
		t.Fatal("expected error")
	}

	apiErr, ok := err.(*runtime.APIError)
	if !ok {
		t.Fatal("expected APIError")
	}

	if diff := cmp.Diff(string(apiErr.Response.(json.RawMessage)), "{\"message\": \"bla\"}"); diff != "" {
		t.Fatal(diff)
	}
}
