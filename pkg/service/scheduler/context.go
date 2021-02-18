// Copyright (C) 2017 ScyllaDB

package scheduler

import "context"

// ctxt is a context key type.
type ctxt byte

// ctxt enumeration.
const (
	ctxOnResume ctxt = iota
)

func markContextAsOnResume(ctx context.Context) context.Context {
	return context.WithValue(ctx, ctxOnResume, true)
}

func isResumeContext(ctx context.Context) bool {
	_, ok := ctx.Value(ctxOnResume).(bool)
	return ok
}
