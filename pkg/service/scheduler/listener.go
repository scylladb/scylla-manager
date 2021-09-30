// Copyright (C) 2017 ScyllaDB

package scheduler

import (
	"context"
	"time"

	"github.com/scylladb/go-log"
	"github.com/scylladb/scylla-manager/pkg/scheduler"
)

type schedulerListener struct {
	scheduler.Listener
	logger log.Logger
}

func newSchedulerListener(logger log.Logger) schedulerListener {
	return schedulerListener{
		Listener: scheduler.NopListener,
		logger:   logger,
	}
}

func (l schedulerListener) OnSchedule(ctx context.Context, key scheduler.Key, begin, end time.Time, retno int8) {
	if end.IsZero() {
		l.logger.Info(ctx, "Schedule", "task_id", key, "begin", begin, "retry", retno)
	} else {
		l.logger.Info(ctx, "Schedule in window", "task_id", key, "begin", begin, "end", end, "retry", retno)
	}
}

func (l schedulerListener) OnUnschedule(ctx context.Context, key scheduler.Key) {
	l.logger.Info(ctx, "Unschedule", "task_id", key)
}

func (l schedulerListener) Trigger(ctx context.Context, key scheduler.Key, success bool) {
	l.logger.Info(ctx, "Trigger", "task_id", key, "success", success)
}

func (l schedulerListener) OnStop(ctx context.Context, key scheduler.Key) {
	l.logger.Info(ctx, "Stop", "task_id", key)
}

func (l schedulerListener) OnRetryBackoff(ctx context.Context, key scheduler.Key, backoff time.Duration, retno int8) {
	l.logger.Info(ctx, "Retry backoff", "task_id", key, "backoff", backoff, "retry", retno)
}

func (l schedulerListener) OnNoTrigger(ctx context.Context, key scheduler.Key) {
	l.logKey(ctx, key, "No trigger")
}

func (l schedulerListener) OnSleep(ctx context.Context, key scheduler.Key, d time.Duration) {
	l.logger.Debug(ctx, "OnSleep", "task_id", key, "duration", d)
}
