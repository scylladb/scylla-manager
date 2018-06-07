// Copyright (C) 2017 ScyllaDB

package mermaidtest

import (
	"testing"
	"time"
)

// WaitCond waits for a condition to become true, checking it in a given
// interval. When wait passes and condition is not met then an error is reported.
func WaitCond(tb testing.TB, cond func() bool, interval, wait time.Duration) {
	tb.Helper()

	if wait <= 0 {
		if !cond() {
			tb.Fatal()
		} else {
			return
		}
	}

	t := time.NewTicker(interval)
	defer t.Stop()
	d := time.NewTimer(wait)
	defer d.Stop()

	for {
		select {
		case <-t.C:
			if cond() {
				return
			}
		case <-d.C:
			tb.Fatal("timeout", wait)
		}
	}
}
