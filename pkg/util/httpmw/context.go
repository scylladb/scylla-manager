// Copyright (C) 2017 ScyllaDB

package httpmw

import "context"

// ctxt is a context key type.
type ctxt byte

// ctxt enumeration.
const (
	ctxHost ctxt = iota
	ctxNoTimeout
)

// ForceHost makes HostPool middleware use the given host instead of selecting
// one.
func ForceHost(ctx context.Context, host string) context.Context {
	return context.WithValue(ctx, ctxHost, host)
}

// IsForceHost checks that ForceHost was applied to the context.
func IsForceHost(ctx context.Context) bool {
	_, ok := ctx.Value(ctxHost).(string)
	return ok
}

// NoTimeout disables Timeout middleware.
func NoTimeout(ctx context.Context) context.Context {
	return context.WithValue(ctx, ctxNoTimeout, true)
}
