// Copyright (C) 2017 ScyllaDB

package parallel

import (
	"testing"
	"time"

	"github.com/scylladb/mermaid/pkg/util/timeutc"
	"go.uber.org/atomic"
)

func TestRun(t *testing.T) {
	t.Parallel()

	const (
		n    = 50
		wait = 5 * time.Millisecond
	)

	table := []struct {
		Name     string
		Limit    int
		Duration time.Duration
	}{
		// This test is flaky under race
		//{
		//	Name:     "No limit",
		//	Duration: wait,
		//},
		{
			Name:     "One by one",
			Limit:    1,
			Duration: n * wait,
		},
		{
			Name:     "Five by five",
			Limit:    5,
			Duration: n / 5 * wait,
		},
	}

	for i := range table {
		test := table[i]

		t.Run(test.Name, func(t *testing.T) {
			t.Parallel()

			active := atomic.NewInt32(0)
			f := func(i int) error {
				v := active.Inc()
				if test.Limit != NoLimit {
					if v > int32(test.Limit) {
						t.Errorf("limit exeded, got %d", v)
					}
				}
				time.Sleep(wait)
				active.Dec()
				return nil
			}

			start := timeutc.Now()
			if err := Run(n, test.Limit, f); err != nil {
				t.Error("Run() error", err)
			}
			d := timeutc.Since(start)
			if a, b := epsilonRange(test.Duration); d < a || d > b {
				t.Errorf("Run() not within expected time margin %v got %v", test.Duration, d)
			}
		})
	}
}

func epsilonRange(d time.Duration) (time.Duration, time.Duration) {
	e := time.Duration(float64(d) * 1.05)
	return d - e, d + e
}
