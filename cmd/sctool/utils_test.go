// Copyright (C) 2017 ScyllaDB

package main

import "testing"

func TestTaskSplit(t *testing.T) {
	tp, id := taskSplit("repair/d7d4b241-f7fe-434e-bc8e-6185b30b078a")
	if tp != "repair" {
		t.Fatal(tp)
	}
	if id != "d7d4b241-f7fe-434e-bc8e-6185b30b078a" {
		t.Fatal(id)
	}
}
