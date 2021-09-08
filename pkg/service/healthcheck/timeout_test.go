// Copyright (C) 2017 ScyllaDB

package healthcheck

import (
	"testing"
	"time"
)

func TestDynamicTimeoutDoNotExceedMaxTimeout(t *testing.T) {
	maxTimeout := time.Second
	probes := 10

	dt := newDynamicTimeout(maxTimeout, probes)

	for i := 0; i < probes; i++ {
		dt.SaveProbe(2 * maxTimeout)
	}

	if dt.Timeout() != maxTimeout {
		t.Errorf("Expected timeout not not exceed max timeout")
	}
}

func TestDynamicTimeoutOverridesOldestProbes(t *testing.T) {
	maxTimeout := time.Second
	probes := 10

	dt := newDynamicTimeout(maxTimeout, probes)

	for i := 0; i < probes; i++ {
		dt.SaveProbe(5 * time.Millisecond)
	}
	for i := 0; i < probes; i++ {
		dt.SaveProbe(10 * time.Millisecond)
	}

	// All probes are equal so stddev is 0 and mean is equal to value of probes
	expectedTimeout := 10*time.Millisecond + minStddev
	if dt.Timeout() != expectedTimeout {
		t.Errorf("Expected timeout equal to %s got %s", expectedTimeout, dt.Timeout())
	}
}

func TestDynamicTimeoutMeanMath(t *testing.T) {
	td := []struct {
		Name         string
		Samples      []time.Duration
		ExpectedMean time.Duration
	}{
		{
			Name:         "empty samples",
			Samples:      []time.Duration{},
			ExpectedMean: 0,
		},
		{
			Name:         "single sample",
			Samples:      []time.Duration{123},
			ExpectedMean: 123,
		},
		{
			Name:         "multiple same values",
			Samples:      []time.Duration{123, 123, 123, 123, 123, 123},
			ExpectedMean: 123,
		},
		{
			Name:         "multiple different values",
			Samples:      []time.Duration{1, 2, 3, 4, 5, 6, 7, 8, 9, 10},
			ExpectedMean: 5,
		},
	}

	for i := range td {
		test := td[i]
		t.Run(test.Name, func(t *testing.T) {
			t.Parallel()

			dt := dynamicTimeout{}
			dt.values = test.Samples

			if test.ExpectedMean != dt.mean() {
				t.Errorf("expected %s mean got %s", test.ExpectedMean, dt.mean())
			}
		})
	}
}

func TestDynamicTimeoutStdDevMath(t *testing.T) {
	td := []struct {
		Name           string
		Samples        []time.Duration
		ExpectedStdDev time.Duration
	}{
		{
			Name:           "empty samples",
			Samples:        []time.Duration{},
			ExpectedStdDev: 0,
		},
		{
			Name:           "single sample",
			Samples:        []time.Duration{123},
			ExpectedStdDev: 0,
		},
		{
			Name:           "multiple same values",
			Samples:        []time.Duration{123, 123, 123, 123, 123, 123},
			ExpectedStdDev: 0,
		},
		{
			Name:           "multiple different values",
			Samples:        []time.Duration{1, 2, 3, 4, 5, 6, 7, 8, 9, 10},
			ExpectedStdDev: 3,
		},
		{
			Name:           "multiple different values",
			Samples:        []time.Duration{1, 1, 2, 3, 4, 1, 2, 3, 2, 1, 2, 3, 1, 4, 1, 5, 1, 6, 1, 6, 1, 7},
			ExpectedStdDev: 2,
		},
		{
			Name:           "multiple different values",
			Samples:        []time.Duration{10, 12, 23, 23, 16, 23, 21, 16},
			ExpectedStdDev: 5,
		},
	}

	for i := range td {
		test := td[i]
		t.Run(test.Name, func(t *testing.T) {
			t.Parallel()

			dt := dynamicTimeout{}
			dt.values = test.Samples

			if test.ExpectedStdDev != dt.stddev() {
				t.Errorf("expected %s stddev got %s", test.ExpectedStdDev, dt.stddev())
			}
		})
	}
}
