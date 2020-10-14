// Copyright (C) 2017 ScyllaDB

package prom

import (
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/scylladb/go-log"
	"go.uber.org/zap/zapcore"
)

func TestMetricsWatcherCallbacks(t *testing.T) {
	mw := NewMetricsWatcher(log.NewDevelopmentWithLevel(zapcore.DebugLevel))
	wrapCalls := make(chan struct{}, 2)
	calls := make(chan struct{}, 5)
	stop1 := mw.RegisterCallback(func() {
		calls <- struct{}{}
	})
	stop2 := mw.RegisterCallback(func() {
		calls <- struct{}{}
	})
	defer stop2()

	rh := mw.WrapHandler(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		wrapCalls <- struct{}{}
	}))
	r := httptest.NewRequest("GET", "/foo", nil)
	w := httptest.NewRecorder()

	rh.ServeHTTP(w, r)
	firstStepCallCount := len(calls)
	if firstStepCallCount != 2 {
		t.Fatalf("Expected two request callback calls, got %d", len(calls))
	}
	stop1()
	rh.ServeHTTP(w, r)
	if len(calls) != firstStepCallCount+2 {
		t.Fatalf("Expected one stop call and one request callback, got %d", len(calls)-firstStepCallCount)
	}
	if len(wrapCalls) != 2 {
		t.Fatalf("Expected two calls to wrapped handler, got %d", len(wrapCalls))
	}
}

func TestMetricsWatcherCallbackTimeout(t *testing.T) {
	defer func(old time.Duration) {
		callbackTimeout = old
	}(callbackTimeout)
	callbackTimeout = 100 * time.Millisecond

	mw := NewMetricsWatcher(log.NewDevelopmentWithLevel(zapcore.DebugLevel))
	wrapCalls := make(chan struct{}, 1)
	calls := make(chan struct{}, 6)
	wait := make(chan struct{})
	stop1 := mw.RegisterCallback(func() {
		<-wait
		calls <- struct{}{}
	})
	defer func() {
		close(wait)
		stop1()
	}()
	stop2 := mw.RegisterCallback(func() {
		calls <- struct{}{}
	})
	defer stop2()
	rh := mw.WrapHandler(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		wrapCalls <- struct{}{}
	}))
	r := httptest.NewRequest("GET", "/foo", nil)
	w := httptest.NewRecorder()
	rh.ServeHTTP(w, r)
	if len(wrapCalls) != 1 {
		t.Fatalf("Expected one call to wrapped handler, got %d", len(wrapCalls))
	}
	callCount := len(calls)
	if callCount != 1 {
		t.Fatalf("Expected one callback call, got %d", callCount)
	}
}
