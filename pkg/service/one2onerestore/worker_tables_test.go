// Copyright (C) 2025 ScyllaDB

package one2onerestore

import (
	"fmt"
	"net/http"
	"sync/atomic"
	"testing"
)

func requireNoError(t *testing.T, err error) {
	t.Helper()
	if err != nil {
		t.Fatalf("Unexpected err: %v", err)
	}
}

func requireError(t *testing.T, err error) {
	t.Helper()
	if err == nil {
		t.Fatalf("Expected err, but got nil")
	}
}

func requireTrue(t *testing.T, b bool, msg string) {
	t.Helper()
	if !b {
		t.Fatal(msg)
	}
}

type testHandler struct {
	http.Handler
	// Keep track of how many times handler func has been called
	// so we can test retries policy.
	calls atomic.Int32
}

func (th *testHandler) ServeHTTP(w http.ResponseWriter, t *http.Request) {
	t.Header.Add("test-call-i", fmt.Sprint(th.calls.Add(1)))
	th.Handler.ServeHTTP(w, t)
}
