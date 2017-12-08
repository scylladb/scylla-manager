// Copyright (C) 2017 ScyllaDB

package scyllaclient

import "context"

// ctxt is a context key type.
type ctxt byte

// ctxt enumeration.
const (
	ctxHost ctxt = iota
)

func withHostPort(ctx context.Context, host string) context.Context {
	return context.WithValue(ctx, ctxHost, withPort(host))
}
