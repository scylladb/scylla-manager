// Copyright (C) 2017 ScyllaDB

package mermaid

import (
	"testing"

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
