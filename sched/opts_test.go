// Copyright (C) 2017 ScyllaDB

package sched

import (
	"testing"
)

func TestSetValue(t *testing.T) {
	p := setValue([]byte("{}"), "foobar", 10)
	if string(p) != `{"foobar":10}` {
		t.Fatal(string(p))
	}
}

func TestSetValuePanicOnError(t *testing.T) {
	defer func() {
		if v := recover(); v == nil {
			t.Fatal("expected panic")
		}
	}()
	setValue(nil, "foobar", 10)
}
