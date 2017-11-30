// Copyright (C) 2017 ScyllaDB

package sched

import (
	"testing"
	"time"
)

func TestRetryFor(t *testing.T) {
	if r := RetryFor(time.Millisecond); r != 0 {
		t.Fatal(r)
	}
	if r := RetryFor(time.Hour); r != 6 {
		t.Fatal(r)
	}
}
