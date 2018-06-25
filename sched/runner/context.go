// Copyright (C) 2017 ScyllaDB

package runner

import "context"

// ctxt is a context key type.
type ctxt byte

// ctxt enumeration.
const (
	ctxOpts ctxt = iota
)

// Opts specifies additional runtime options, usually those are user provided
// when manually starting a task.
type Opts struct {
	Continue bool
}

// DefaultOpts is returned by OptsFromContext when there is no Opts set.
var DefaultOpts = Opts{
	Continue: true,
}

// WithOpts returns a copy of context with Opts set, to retrieve it use
// OptsFromContext.
func WithOpts(parent context.Context, opts Opts) context.Context {
	return context.WithValue(parent, ctxOpts, opts)
}

// OptsFromContext returns Opts from context or DefaultOpts if nothing was set.
func OptsFromContext(ctx context.Context) Opts {
	opts, ok := ctx.Value(ctxOpts).(Opts)
	if ok {
		return opts
	}
	return DefaultOpts
}
