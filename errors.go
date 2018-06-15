// Copyright (C) 2017 ScyllaDB

package mermaid

import (
	"fmt"

	"github.com/gocql/gocql"
	"github.com/pkg/errors"
)

// Common errors
var (
	ErrNotFound = gocql.ErrNotFound
	ErrNilPtr   = errors.New("nil")
)

// ErrValidate marks error as a validation error, if err is not nil.
func ErrValidate(err error, msg string) error {
	if err == nil {
		return nil
	}
	return errValidate{
		msg: msg,
		err: err,
	}
}

// IsErrValidate checks if given error is a validation error.
func IsErrValidate(err error) bool {
	_, ok := err.(errValidate)
	return ok
}

// errValidate is a validation error caused by inner error.
type errValidate struct {
	msg string
	err error
}

// Error implements error.
func (e errValidate) Error() string {
	if e.msg == "" {
		return e.err.Error()
	}

	return fmt.Sprintf("%s: %s", e.msg, e.err)
}
