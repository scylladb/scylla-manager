// Copyright (C) 2017 ScyllaDB

package sched

import (
	"github.com/scylladb/mermaid/uuid"
	"time"
)

// SetRetryTaskWait allows for setting retryTaskWait in an integration test.
func SetRetryTaskWait(d time.Duration) {
	retryTaskWait = d
}

// SetStopTaskWait allows for setting stopTaskWait in an integration test.
func SetStopTaskWait(d time.Duration) {
	stopTaskWait = d
}

// SetStartTaskNowSlack allows for setting startTaskNowSlack in an integration test.
func SetStartTaskNowSlack(d time.Duration) {
	startTaskNowSlack = d
}

// NewRun exposes newRun for testing.
func (t *Task) NewRun() *Run {
	return t.newRun(uuid.NewTime())
}
