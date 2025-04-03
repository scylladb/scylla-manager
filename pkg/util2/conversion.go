// Copyright (C) 2025 ScyllaDB

package util2

import "fmt"

// ConvertSlice returns slice obtained by calling 'f' on elements of 'in'.
func ConvertSlice[T, V any](in []T, f func(T) V) []V {
	out := make([]V, len(in))
	for i, t := range in {
		out[i] = f(t)
	}
	return out
}

// ConvertSliceWithError returns slice obtained by calling 'f' on elements of 'in'.
func ConvertSliceWithError[T, V any](in []T, f func(T) (V, error)) ([]V, error) {
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

// ConvertSliceToString returns slice obtained by calling 'String()' on elements of 'in'.
func ConvertSliceToString[T fmt.Stringer](in []T) []string {
	out := make([]string, len(in))
	for i, t := range in {
		out[i] = t.String()
	}
	return out
}
