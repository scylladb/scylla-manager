// Copyright (C) 2017 ScyllaDB

package mermaid

import (
	"testing"

	"github.com/google/go-cmp/cmp"
)

func TestUniq(t *testing.T) {
	u := Uniq{}
	u.Put("b")
	u.Put("a")
	u.Put("a")

	if diff := cmp.Diff(u.Slice(), []string{"a", "b"}); diff != "" {
		t.Fatal(diff)
	}

	if !u.Has("a") {
		t.Fatal("a")
	}

	if u.Has("c") {
		t.Fatal("a")
	}
}
