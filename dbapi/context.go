// Copyright (C) 2017 ScyllaDB

package dbapi

import "context"

// ctxt is a context key type.
type ctxt byte

// ctxt enumeration.
const (
	_host ctxt = iota
)

func withHost(ctx context.Context, host string) context.Context {
	return context.WithValue(ctx, _host, host)
}
