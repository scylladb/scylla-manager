// Copyright (C) 2017 ScyllaDB

package prom

import (
	"net/http"
	"sync"
	"time"

	"github.com/scylladb/go-log"
	"github.com/scylladb/scylla-manager/pkg/util/parallel"
)

// MetricsWatcher keeps track of registered callbacks for metrics requests.
type MetricsWatcher struct {
	logger log.Logger

	mu        sync.Mutex
	callbacks []*func()
}

func NewMetricsWatcher(logger log.Logger) *MetricsWatcher {
	return &MetricsWatcher{logger: logger}
}

// Default scrape request timeout for Prometheus is 10s.
// This variable will determine how long to wait for all registered callbacks
// to execute before continuing with the request.
// 8s is chosen to leave room for other operations.
var callbackTimeout = 8 * time.Second

// WrapHandler returns function that will execute registered callbacks when
// http request comes in.
// Registered callbacks are executed concurrently.
// If there is no progress in registered callbacks for at least two seconds
// Request will be served without waiting for callbacks to complete.
func (mw *MetricsWatcher) WrapHandler(handler http.Handler) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		mw.mu.Lock()
		callbacks := make([]*func(), len(mw.callbacks))
		copy(callbacks, mw.callbacks)
		mw.mu.Unlock()

		done := make(chan struct{}, len(callbacks))
		t := time.NewTimer(callbackTimeout)
		defer t.Stop()

		go func() {
			if err := parallel.Run(len(callbacks), 0, func(i int) error {
				(*callbacks[i])()
				done <- struct{}{}
				return nil
			}); err != nil {
				mw.logger.Error(r.Context(), "Failed to execute callbacks", "error", err)
			}
		}()

	loop:
		for range callbacks {
			select {
			case <-done:
			case <-t.C:
				mw.logger.Error(r.Context(), "Timeout waiting for metrics callbacks")
				break loop
			}
		}
		handler.ServeHTTP(w, r)
	}
}

// RegisterCallback registers and calls callback to be executed when metrics
// are requested.
// It returns function that should be called to stop watching for requests.
// When stop function is called callback will be executed.
func (mw *MetricsWatcher) RegisterCallback(callback func()) (unregister func()) {
	mw.mu.Lock()
	defer mw.mu.Unlock()

	mw.callbacks = append(mw.callbacks, &callback)

	return func() {
		mw.mu.Lock()
		defer mw.mu.Unlock()
		for i := range mw.callbacks {
			if mw.callbacks[i] == &callback {
				mw.callbacks = append(mw.callbacks[:i], mw.callbacks[i+1:]...)
				callback()
				return
			}
		}
	}
}
