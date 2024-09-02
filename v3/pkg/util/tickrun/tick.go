// Copyright (C) 2017 ScyllaDB

package tickrun

import (
	"time"
)

// NewTicker executes provided `f` function on every `d` tick.
// It spawns goroutine which can be stopped using returned `stop` function.
//
// Upon `stop` function call, `f` will be executed once for the last time.
func NewTicker(d time.Duration, f func()) func() {
	ticker := time.NewTicker(d)
	c := ticker.C

	if overrideTickerChanTestHook != nil {
		c = overrideTickerChanTestHook()
	}

	done := make(chan struct{})
	stop := make(chan struct{})

	go func() {
		defer close(done)
		for {
			select {
			case <-stop:
				return
			case <-c:
				f()
			}
		}
	}()

	return func() {
		ticker.Stop()
		close(stop)
		<-done
		f()
	}
}

// for test purposes.
var overrideTickerChanTestHook func() <-chan time.Time
