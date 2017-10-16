// Copyright (C) 2017 ScyllaDB

package mermaid

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/scylladb/mermaid/uuid"
)

func TestClientClusterUUID(t *testing.T) {
	t.Parallel()

	u := uuid.MustRandom()
	c := NewClient("", u.String())
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

	c := NewClient(s.Listener.Addr().String(), uuid.MustRandom().String())
	_, err := c.ListRepairUnits(context.Background())
	if err == nil {
		t.Fatal("expected error")
	}

	if diff := cmp.Diff(err.Error(), "API error (status 500): \n{\n  \"message\": \"bla\"\n}\n "); diff != "" {
		t.Fatal(diff)
	}
}
