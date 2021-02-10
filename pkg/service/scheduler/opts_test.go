// Copyright (C) 2017 ScyllaDB

package scheduler

import (
	"testing"
)

func TestPropertiesSet(t *testing.T) {
	p := Properties("{}").Set("foobar", 10)
	if string(p) != `{"foobar":10}` {
		t.Fatal(string(p))
	}
}

func TestPropertiesSetPanicOnError(t *testing.T) {
	defer func() {
		if v := recover(); v == nil {
			t.Fatal("expected panic")
		}
	}()
	Properties(nil).Set("foobar", 10)
}
