// Copyright (C) 2017 ScyllaDB

package scheduler

import (
	"time"

	"github.com/scylladb/scylla-manager/pkg/util/uuid"
)

func SetRetryTaskWait(d time.Duration) {
	retryTaskWait = d
}

func SetStopTaskWait(d time.Duration) {
	stopTaskWait = d
}

func (t *Task) NewRun() *Run {
	return t.newRun(uuid.NewTime())
}
