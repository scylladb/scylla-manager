// Copyright (C) 2017 ScyllaDB

package healthcheck

import (
	"math/rand"
	"testing"
	"time"

	"github.com/scylladb/scylla-manager/v3/pkg/testutils"
)

func TestDynamicTimeoutCalculateTimeout(t *testing.T) {
	relativeTimeout := time.Duration(10)
	maxTimeout := time.Duration(20)
	probes := 5

	table := []struct {
		Name   string
		Probes []time.Duration
		Golden time.Duration
	}{
		{
			Name:   "Simple",
			Probes: []time.Duration{1, 1, 2, 100, 100},
			Golden: relativeTimeout + time.Duration(2),
		},
		{
			Name:   "Over max",
			Probes: []time.Duration{1, 1, 100, 100, 100},
			Golden: maxTimeout,
		},
	}

	dt := newDynamicTimeout(relativeTimeout, maxTimeout, probes)

	for _, test := range table {
		copy(dt.probes, test.Probes)
		timeout := dt.calculateTimeout()
		if timeout != test.Golden {
			t.Errorf("calculateTimeout()=%s, expected %s", timeout, test.Golden)
		}
	}
}

func TestDynamicTimeoutRecord(t *testing.T) {
	relativeTimeout := time.Duration(100)
	maxTimeout := time.Duration(1000)
	probes := 50

	dt := newDynamicTimeout(relativeTimeout, maxTimeout, probes)
	r := rand.New(rand.NewSource(1944))

	// Fill with U[0, 100) distribution, expected t/o is 50 + 100 (relativeTimeout)
	for i := 0; i < 100; i++ {
		dt.Record(time.Duration(r.Intn(100)))
	}
	t0 := dt.Timeout()
	if a, b := testutils.EpsilonRange(150); t0 < a || t0 > b {
		t.Fatalf("dt.Timeout()=%s, expected %d", t0, 150)
	}

	// Fill with U[0, 300) distribution, expected t/o is 150 + 100 (relativeTimeout)
	for i := 0; i < 100; i++ {
		dt.Record(time.Duration(r.Intn(300)))
	}
	t1 := dt.Timeout()
	if a, b := testutils.EpsilonRange(250); t1 < a || t1 > b {
		t.Fatalf("dt.Timeout()=%s, expected %d", t0, 250)
	}
}
