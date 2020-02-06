// Copyright (C) 2017 ScyllaDB

package scyllaclient

import "context"

// ctxt is a context key type.
type ctxt byte

// ctxt enumeration.
const (
	ctxNoRetry ctxt = iota
)

// NoRetry disables Retry middleware.
func NoRetry(ctx context.Context) context.Context {
	return context.WithValue(ctx, ctxNoRetry, true)
}
