// Copyright (C) 2017 ScyllaDB

package sched

import "time"

// SetRetryTaskWait allows for setting retryTaskWait in an integration test.
func SetRetryTaskWait(d time.Duration) {
	retryTaskWait = d
}

// SetTaskStartNowSlack allows for setting taskStartNowSlack in an
// integration test.
func SetTaskStartNowSlack(d time.Duration) {
	taskStartNowSlack = d
}

// SetMonitorTaskInterval allows for setting monitorTaskInterval in an
// integration test.
func SetMonitorTaskInterval(d time.Duration) {
	monitorTaskInterval = d
}
