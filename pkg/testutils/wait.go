// Copyright (C) 2017 ScyllaDB

package testutils

import (
	"testing"
	"time"
)

// WaitCond waits for a condition to become true, checking it in a given
// interval. When wait passes and condition is not met then an error is reported.
func WaitCond(t *testing.T, cond func() bool, interval, wait time.Duration) {
	t.Helper()

	if wait <= 0 {
		if !cond() {
			t.Fatal()
		} else {
			return
		}
	}

	c := time.NewTicker(interval)
	defer c.Stop()
	d := time.NewTimer(wait)
	defer d.Stop()

	for {
		select {
		case <-c.C:
			if cond() {
				return
			}
		case <-d.C:
			t.Fatal("timeout", wait)
		}
	}
}
