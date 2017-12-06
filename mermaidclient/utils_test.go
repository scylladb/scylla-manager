// Copyright (C) 2017 ScyllaDB

package mermaidclient

import (
	"testing"

	"github.com/scylladb/mermaid/uuid"
)

func TestExtractIDFromLocation(t *testing.T) {
	t.Parallel()

	u0 := uuid.MustRandom()
	u1, err := extractIDFromLocation("http://bla/bla/" + u0.String() + "?param=true")
	if err != nil {
		t.Fatal(err)
	}
	if u1 != u0 {
		t.Fatal(u1, u0)
	}
}

func TestExtractTaskIDFromLocation(t *testing.T) {
	t.Parallel()

	u0 := uuid.MustRandom()
	u1, err := extractTaskIDFromLocation("http://bla/bla/?task_id=" + u0.String())
	if err != nil {
		t.Fatal(err)
	}
	if u1 != u0 {
		t.Fatal(u1, u0)
	}
}
