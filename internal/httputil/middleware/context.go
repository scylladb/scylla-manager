// Copyright (C) 2017 ScyllaDB

package middleware

import "context"

// ctxt is a context key type.
type ctxt byte

// ctxt enumeration.
const (
	ctxHost ctxt = iota
	ctxNoRetry
	ctxDontLog
)

// ForceHost makes HostPool middleware use the given host instead of selecting
// one.
func ForceHost(ctx context.Context, host string) context.Context {
	return context.WithValue(ctx, ctxHost, host)
}

// NoRetry disables Retry middleware.
func NoRetry(ctx context.Context) context.Context {
	return context.WithValue(ctx, ctxNoRetry, true)
}

// DontLog disables Logger middleware.
func DontLog(ctx context.Context) context.Context {
	return context.WithValue(ctx, ctxDontLog, true)
}
