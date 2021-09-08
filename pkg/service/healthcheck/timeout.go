// Copyright (C) 2017 ScyllaDB

package healthcheck

import (
	"sort"
	"sync"
	"time"
)

type dynamicTimeout struct {
	relativeTimeout time.Duration
	maxTimeout      time.Duration

	probes                []time.Duration
	idx                   int
	sortedProbes          []time.Duration
	timeoutRecalculateIdx int
	timeout               time.Duration
	mu                    sync.RWMutex
}

func newDynamicTimeout(relativeTimeout, maxTimeout time.Duration, probes int) *dynamicTimeout {
	return &dynamicTimeout{
		relativeTimeout:       relativeTimeout,
		maxTimeout:            maxTimeout,
		probes:                make([]time.Duration, probes),
		sortedProbes:          make([]time.Duration, probes),
		idx:                   0,
		timeoutRecalculateIdx: probes - 1,
		timeout:               maxTimeout,
	}
}

func (dt *dynamicTimeout) Timeout() time.Duration {
	dt.mu.RLock()
	defer dt.mu.RUnlock()
	return dt.timeout
}

func (dt *dynamicTimeout) Record(rtt time.Duration) {
	dt.mu.Lock()
	defer dt.mu.Unlock()

	// Save probe
	dt.probes[dt.idx] = rtt

	n := len(dt.probes)

	// Calculate next index
	dt.idx = (dt.idx + 1) % n

	// Recalculate timeout if needed
	if dt.idx == dt.timeoutRecalculateIdx {
		dt.timeout = dt.calculateTimeoutLocked()
		dt.timeoutRecalculateIdx = (dt.idx + (n / 10)) % n
	}
}

func (dt *dynamicTimeout) calculateTimeoutLocked() time.Duration {
	timeout := dt.medianLocked() + dt.relativeTimeout
	if timeout > dt.maxTimeout {
		timeout = dt.maxTimeout
	}
	return timeout
}

func (dt *dynamicTimeout) medianLocked() time.Duration {
	copy(dt.sortedProbes, dt.probes)
	sort.Slice(dt.sortedProbes, func(i, j int) bool {
		return dt.sortedProbes[i] < dt.sortedProbes[j]
	})
	return dt.sortedProbes[len(dt.sortedProbes)/2]
}
