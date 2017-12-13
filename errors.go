// Copyright (C) 2017 ScyllaDB

package mermaid

import (
	"github.com/gocql/gocql"
	"github.com/pkg/errors"
)

// Common errors
var (
	ErrNotFound = gocql.ErrNotFound
	ErrNilPtr   = errors.New("nil")
)

// ParamError marks error as caused by invalid parameter passed to a function.
type ParamError struct {
	Cause error
}

// Error implements error.
func (e ParamError) Error() string {
	if e.Cause == nil {
		return ""
	}
	return e.Cause.Error()
}
