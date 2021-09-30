// Copyright (C) 2017 ScyllaDB

package scheduler

import (
	"time"

	"github.com/scylladb/scylla-manager/pkg/scheduler"
	"github.com/scylladb/scylla-manager/pkg/util/duration"
	"github.com/scylladb/scylla-manager/pkg/util/retry"
)

func details(t *Task) scheduler.Details {
	return scheduler.Details{
		Properties: t.Properties,
		Backoff:    backoff(t),
		Trigger:    t.Sched.trigger(),
	}
}

func backoff(t *Task) retry.Backoff {
	if t.Sched.NumRetries == 0 {
		return nil
	}
	interval := t.Sched.RetryInitialInterval
	if interval == 0 {
		interval = duration.Duration(10 * time.Minute)
	}

	b := retry.NewExponentialBackoff(interval.Duration(), 0, 0, 2, 0)
	b = retry.WithMaxRetries(b, uint64(t.Sched.NumRetries))
	return b
}
