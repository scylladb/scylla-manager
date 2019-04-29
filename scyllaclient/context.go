// Copyright (C) 2017 ScyllaDB

package scyllaclient

import "context"

// ctxt is a context key type.
type ctxt byte

// ctxt enumeration.
const (
	ctxHost ctxt = iota
	ctxNoRetry
)

func forceHostPort(ctx context.Context, host, port string) context.Context {
	return context.WithValue(ctx, ctxHost, withPort(host, port))
}

func noRetry(ctx context.Context) context.Context {
	return context.WithValue(ctx, ctxNoRetry, true)
}
