// Copyright (C) 2017 ScyllaDB

package scheduler

import (
	"context"
	"time"
)

// Listener specifies pluggable hooks for scheduler events.
type Listener interface {
	OnSchedulerStart(ctx context.Context)
	OnSchedulerStop(ctx context.Context)
	OnRunStart(ctx *RunContext)
	OnRunSuccess(ctx *RunContext)
	OnRunStop(ctx *RunContext)
	OnRunWindowEnd(ctx *RunContext)
	OnRunError(ctx *RunContext, err error)
	OnSchedule(ctx context.Context, key Key, begin, end time.Time, retno int8)
	OnUnschedule(ctx context.Context, key Key)
	OnTrigger(ctx context.Context, key Key, success bool)
	OnStop(ctx context.Context, key Key)
	OnRetryBackoff(ctx context.Context, key Key, backoff time.Duration, retno int8)
	OnNoTrigger(ctx context.Context, key Key)
	OnSleep(ctx context.Context, key Key, d time.Duration)
}

type nopListener struct{}

func (l nopListener) OnSchedulerStart(ctx context.Context) {
}

func (l nopListener) OnSchedulerStop(ctx context.Context) {
}

func (l nopListener) OnRunStart(ctx *RunContext) {
}

func (l nopListener) OnRunSuccess(ctx *RunContext) {
}

func (l nopListener) OnRunStop(ctx *RunContext) {
}

func (l nopListener) OnRunWindowEnd(ctx *RunContext) {
}

func (l nopListener) OnRunError(ctx *RunContext, err error) {
}

func (l nopListener) OnSchedule(ctx context.Context, key Key, begin, end time.Time, retno int8) {
}

func (l nopListener) OnUnschedule(ctx context.Context, key Key) {
}

func (l nopListener) OnTrigger(ctx context.Context, key Key, success bool) {
}

func (l nopListener) OnStop(ctx context.Context, key Key) {
}

func (l nopListener) OnRetryBackoff(ctx context.Context, key Key, backoff time.Duration, retno int8) {
}

func (l nopListener) OnNoTrigger(ctx context.Context, key Key) {
}

func (l nopListener) OnSleep(ctx context.Context, key Key, d time.Duration) {
}

// NopListener is a Listener implementation that has no effects.
var NopListener = nopListener{}
