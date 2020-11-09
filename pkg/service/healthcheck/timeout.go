// Copyright (C) 2017 ScyllaDB

package healthcheck

import (
	"math"
	"sync"
	"time"

	"github.com/scylladb/scylla-manager/pkg/util/uuid"
)

// TimeoutProviderFunc is a function which returns probe timeout for given DC
// and function which should be called with next probe result.
type timeoutProviderFunc func(clusterID uuid.UUID, dc string) (timeout time.Duration, saveNext func(time.Duration))

type dynamicTimeout struct {
	config      DynamicTimeoutConfig
	metricsHook func(mean, stddev, delta time.Duration) // Values are in milliseconds

	mu     sync.Mutex
	values []time.Duration
	idx    int
}

func newDynamicTimeout(config DynamicTimeoutConfig, metricsHook func(mean, stddev, noise time.Duration)) *dynamicTimeout {
	values := make([]time.Duration, 0, config.Probes)
	return &dynamicTimeout{
		config:      config,
		values:      values,
		metricsHook: metricsHook,
	}
}

func (dt *dynamicTimeout) mean() time.Duration {
	if len(dt.values) == 0 {
		return 0
	}
	var m time.Duration
	for _, v := range dt.values {
		m += v
	}
	return m / time.Duration(len(dt.values))
}

func (dt *dynamicTimeout) stddev() time.Duration {
	if len(dt.values) < 2 {
		return 0
	}

	m := dt.mean()

	var sd time.Duration
	for _, v := range dt.values {
		sd += time.Duration(math.Pow(float64(v-m), 2))
	}
	sd = time.Duration(math.Sqrt(float64(sd / time.Duration(len(dt.values)-1))))
	if sd <= 0 {
		return 0
	}
	return sd
}

const minStddev = 1 * time.Millisecond

func (dt *dynamicTimeout) Timeout() time.Duration {
	dt.mu.Lock()
	defer dt.mu.Unlock()

	return dt.timeout()
}

func (dt *dynamicTimeout) timeout() time.Duration {
	// Calculate dynamic timeout once we collect 10% of total
	// required probes. Otherwise return max_timeout.
	if len(dt.values) < int(0.1*float64(dt.config.Probes)) {
		return dt.config.MaxTimeout
	}

	sd := dt.stddev()
	m := dt.mean()

	delta := time.Duration(dt.config.StdDevMultiplier) * max(sd, minStddev)
	timeout := m + delta

	if dt.metricsHook != nil {
		dt.metricsHook(m, sd, delta)
	}

	if dt.config.MaxTimeout != 0 && timeout > dt.config.MaxTimeout {
		timeout = dt.config.MaxTimeout
	}
	return timeout
}

func (dt *dynamicTimeout) SaveProbe(probe time.Duration) {
	dt.mu.Lock()
	defer dt.mu.Unlock()

	if len(dt.values) < dt.config.Probes {
		dt.values = append(dt.values, probe)
	} else {
		dt.values[dt.idx] = probe
		dt.idx = (dt.idx + 1) % len(dt.values)
	}
}

func max(a, b time.Duration) time.Duration {
	if a > b {
		return a
	}
	return b
}
