// Copyright (C) 2017 ScyllaDB

package scheduler

import (
	"context"
	"time"
)

// Listener specifies pluggable hooks for scheduler events.
// Parametrized by scheduler key type.
type Listener[K comparable] interface {
	OnSchedulerStart(ctx context.Context)
	OnSchedulerStop(ctx context.Context)
	OnRunStart(ctx *RunContext[K])
	OnRunSuccess(ctx *RunContext[K])
	OnRunStop(ctx *RunContext[K])
	OnRunWindowEnd(ctx *RunContext[K])
	OnRunError(ctx *RunContext[K], err error)
	OnSchedule(ctx context.Context, key K, begin, end time.Time, retno int8)
	OnUnschedule(ctx context.Context, key K)
	OnTrigger(ctx context.Context, key K, success bool)
	OnStop(ctx context.Context, key K)
	OnRetryBackoff(ctx context.Context, key K, backoff time.Duration, retno int8)
	OnNoTrigger(ctx context.Context, key K)
	OnSleep(ctx context.Context, key K, d time.Duration)
}

type nopListener[K comparable] struct{}

func (l nopListener[_]) OnSchedulerStart(ctx context.Context) {
}

func (l nopListener[_]) OnSchedulerStop(ctx context.Context) {
}

func (l nopListener[K]) OnRunStart(ctx *RunContext[K]) {
}

func (l nopListener[K]) OnRunSuccess(ctx *RunContext[K]) {
}

func (l nopListener[K]) OnRunStop(ctx *RunContext[K]) {
}

func (l nopListener[K]) OnRunWindowEnd(ctx *RunContext[K]) {
}

func (l nopListener[K]) OnRunError(ctx *RunContext[K], err error) {
}

func (l nopListener[K]) OnSchedule(ctx context.Context, key K, begin, end time.Time, retno int8) {
}

func (l nopListener[K]) OnUnschedule(ctx context.Context, key K) {
}

func (l nopListener[K]) OnTrigger(ctx context.Context, key K, success bool) {
}

func (l nopListener[K]) OnStop(ctx context.Context, key K) {
}

func (l nopListener[K]) OnRetryBackoff(ctx context.Context, key K, backoff time.Duration, retno int8) {
}

func (l nopListener[K]) OnNoTrigger(ctx context.Context, key K) {
}

func (l nopListener[K]) OnSleep(ctx context.Context, key K, d time.Duration) {
}

// NopListener returns a Listener implementation that has no effects.
func NopListener[K comparable]() Listener[K] {
	return nopListener[K]{}
}
