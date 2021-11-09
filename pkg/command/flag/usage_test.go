// Copyright (C) 2017 ScyllaDB

package flag

import "testing"

func TestUsage(t *testing.T) {
	if len(usage) == 0 {
		t.Fatal("Expected usage")
	}
}
