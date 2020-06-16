// Copyright (C) 2017 ScyllaDB

package ping

import "errors"

var (
	// ErrTimeout is returned when ping times out.
	ErrTimeout = errors.New("timeout")

	// ErrUnauthorised is returned when wrong credentials are passed.
	ErrUnauthorised = errors.New("unauthorised")
)
