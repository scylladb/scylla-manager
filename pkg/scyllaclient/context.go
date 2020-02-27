// Copyright (C) 2017 ScyllaDB

package scyllaclient

import "context"

// ctxt is a context key type.
type ctxt byte

// ctxt enumeration.
const (
	ctxInteractive ctxt = iota
	ctxHost
	ctxNoRetry
	ctxNoTimeout
)

// Interactive context means that it should be processed fast without too much
// useless waiting.
func Interactive(ctx context.Context) context.Context {
	return context.WithValue(ctx, ctxInteractive, true)
}

func isInteractive(ctx context.Context) bool {
	_, ok := ctx.Value(ctxInteractive).(bool)
	return ok
}

// forceHost makes hostPool middleware use the given host instead of selecting
// one.
func forceHost(ctx context.Context, host string) context.Context {
	return context.WithValue(ctx, ctxHost, host)
}

func isForceHost(ctx context.Context) bool {
	_, ok := ctx.Value(ctxHost).(string)
	return ok
}

// NoRetry disables Retry middleware.
func NoRetry(ctx context.Context) context.Context {
	return context.WithValue(ctx, ctxNoRetry, true)
}

// noTimeout disables Timeout middleware.
//
// WARNING: Usually this is a workaround for Scylla or other API slowness
// in field condition i.e. with tons of data. This is the last resort of
// defense please use with care.
func noTimeout(ctx context.Context) context.Context {
	return context.WithValue(ctx, ctxNoTimeout, true)
}
