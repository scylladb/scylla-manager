// Copyright (C) 2025 ScyllaDB

package slices

import (
	"fmt"
)

// Map returns slice obtained by calling 'f' on elements of 'in'.
func Map[T, V any](in []T, f func(T) V) []V {
	out := make([]V, len(in))
	for i, t := range in {
		out[i] = f(t)
	}
	return out
}

// MapWithError returns slice obtained by calling 'f' on elements of 'in'.
func MapWithError[T, V any](in []T, f func(T) (V, error)) ([]V, error) {
	out := make([]V, len(in))
	for i, t := range in {
		v, err := f(t)
		if err != nil {
			return nil, err
		}
		out[i] = v
	}
	return out, nil
}

// MapToString returns slice obtained by calling 'String()' on elements of 'in'.
func MapToString[T fmt.Stringer](in []T) []string {
	out := make([]string, len(in))
	for i, t := range in {
		out[i] = t.String()
	}
	return out
}
