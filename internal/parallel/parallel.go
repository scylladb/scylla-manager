// Copyright (C) 2017 ScyllaDB

package parallel

import (
	"go.uber.org/atomic"
	"go.uber.org/multierr"
)

// NoLimit means full parallelism mode.
const NoLimit = 0

// Run executes function f with arguments ranging from 0 to n-1 executing at
// most limit in parallel.
// If limit is 0 it runs f(0),f(1),...,f(n-1) in parallel.
func Run(n, limit int, f func(i int) error) error {
	if limit <= 0 || limit > n {
		limit = n
	}

	idx := atomic.NewInt32(0)
	out := make(chan error)
	for j := 0; j < limit; j++ {
		go func() {
			for {
				i := int(idx.Inc()) - 1
				if i >= n {
					return
				}
				out <- f(i)
			}
		}()
	}

	var errs error
	for i := 0; i < n; i++ {
		errs = multierr.Append(errs, <-out)
	}
	return errs
}
