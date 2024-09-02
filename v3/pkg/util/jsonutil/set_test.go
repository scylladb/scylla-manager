// Copyright (C) 2017 ScyllaDB

package jsonutil

import (
	"encoding/json"
	"testing"
)

func TestSet(t *testing.T) {
	p := Set(json.RawMessage("{}"), "foobar", 10)
	if string(p) != `{"foobar":10}` {
		t.Fatal(string(p))
	}
}

func TestSetPanicOnError(t *testing.T) {
	defer func() {
		if v := recover(); v == nil {
			t.Fatal("expected panic")
		}
	}()
	Set(json.RawMessage(nil), "foobar", 10)
}
