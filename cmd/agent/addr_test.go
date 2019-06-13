// Copyright (C) 2017 ScyllaDB

package main

import "testing"

func TestGuessAddr(t *testing.T) {
	addrs := guessAddr()
	if len(addrs) == 0 {
		t.Fatal("expected addresses")
	}
}
