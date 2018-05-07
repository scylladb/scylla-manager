// Copyright (C) 2017 ScyllaDB

package golog

// ctxt is a context key type.
type ctxt byte

// ctxt enumeration.
const (
	ctxTraceID ctxt = iota
)
