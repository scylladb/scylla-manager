// Copyright (C) 2017 ScyllaDB

package mermaidclient

import (
	"testing"

	"github.com/scylladb/mermaid/uuid"
)

func TestUUIDFromLocation(t *testing.T) {
	t.Parallel()

	u0 := uuid.MustRandom()
	u1, err := uuidFromLocation("http://bla/bla/" + u0.String() + "?param=true")
	if err != nil {
		t.Fatal(err)
	}
	if u1 != u0 {
		t.Fatal(u1, u0)
	}
}
