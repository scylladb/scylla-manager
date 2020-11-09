// Copyright (C) 2017 ScyllaDB

package tickrun

import (
	"sync/atomic"
	"testing"
	"time"

	"github.com/scylladb/scylla-manager/pkg/util/timeutc"
)

type syncClock struct {
	c chan time.Time
}

func (t *syncClock) Tick() {
	t.c <- timeutc.Now()
}

func newSyncClock() *syncClock {
	return &syncClock{c: make(chan time.Time)}
}

func eventuallyHaveValue(counter *int64, expectedValue int) bool {
	const retries = 3
	for i := 0; i < retries; i++ {
		v := atomic.LoadInt64(counter)
		if v != int64(expectedValue) {
			time.Sleep(100 * time.Millisecond)
		} else {
			return true
		}
	}
	return false
}

func testTicker(f func(ticker *syncClock, callCounter *int64, stop func())) {
	clock := newSyncClock()

	OverrideTickerChan(clock.c)

	callCounter := int64(0)
	stop := NewTicker(time.Second, func() {
		atomic.AddInt64(&callCounter, 1)
	})

	f(clock, &callCounter, stop)
}

func TestTickerCallFuncOnEveryClockTick(t *testing.T) {
	testTicker(func(clock *syncClock, callCounter *int64, stop func()) {
		defer stop()

		clock.Tick()
		if !eventuallyHaveValue(callCounter, 1) {
			t.Error()
		}

		clock.Tick()
		if !eventuallyHaveValue(callCounter, 2) {
			t.Error()
		}
	})
}

func TestTickerCallFuncLastTimeOnStop(t *testing.T) {
	testTicker(func(_ *syncClock, callCounter *int64, stop func()) {
		stop()
		if !eventuallyHaveValue(callCounter, 1) {
			t.Error()
		}
	})
}
