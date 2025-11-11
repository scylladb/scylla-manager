// Copyright (C) 2025 ScyllaDB

package testutils

import (
	"testing"
	"time"

	"github.com/pkg/errors"
)

// WaitCond is a wrapper for WaitCondError which fails the test in case of an error.
func WaitCond(t *testing.T, cond func() bool, interval, wait time.Duration) {
	t.Helper()

	if err := WaitCondError(cond, interval, wait); err != nil {
		t.Fatal(err)
	}
}

// WaitCondError waits for a condition to become true, checking it in a given
// interval. When wait passes and condition is not met then an error is returned.
func WaitCondError(cond func() bool, interval, wait time.Duration) error {
	if wait <= 0 {
		if !cond() {
			return errors.New("condition not met initially")
		}
		return nil
	}

	c := time.NewTicker(interval)
	defer c.Stop()
	d := time.NewTimer(wait)
	defer d.Stop()

	for {
		select {
		case <-c.C:
			if cond() {
				return nil
			}
		case <-d.C:
			return errors.Errorf("condition not met within timeout %v", wait)
		}
	}
}
