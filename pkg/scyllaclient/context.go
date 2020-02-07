// Copyright (C) 2017 ScyllaDB

package scyllaclient

import "context"

// ctxt is a context key type.
type ctxt byte

// ctxt enumeration.
const (
	ctxHost ctxt = iota
	ctxNoRetry
	ctxNoTimeout
)

// forceHost makes hostPool middleware use the given host instead of selecting
// one.
func forceHost(ctx context.Context, host string) context.Context {
	return context.WithValue(ctx, ctxHost, host)
}

// isForceHost checks that forceHost was applied to the context.
func isForceHost(ctx context.Context) bool {
	_, ok := ctx.Value(ctxHost).(string)
	return ok
}

// NoRetry disables Retry middleware.
func NoRetry(ctx context.Context) context.Context {
	return context.WithValue(ctx, ctxNoRetry, true)
}

// NoTimeout disables Timeout middleware.
//
// WARNING: Usually this is a workaround for Scylla or other API slowness
// in field condition i.e. with tons of data. This is the last resort of
// defense please use with care.
func NoTimeout(ctx context.Context) context.Context {
	return context.WithValue(ctx, ctxNoTimeout, true)
}
