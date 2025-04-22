// Copyright (C) 2025 ScyllaDB

package maps

// SetFromSlice returns set with elements of 'in'.
func SetFromSlice[T comparable](in []T) map[T]struct{} {
	out := make(map[T]struct{}, len(in))
	for _, t := range in {
		out[t] = struct{}{}
	}
	return out
}

// MapKey returns map obtained by calling 'f' on keys of 'in'.
func MapKey[T, K comparable, V any](in map[T]V, f func(T) K) map[K]V {
	out := make(map[K]V, len(in))
	for t, v := range in {
		out[f(t)] = v
	}
	return out
}

// MapKeyWithError returns map obtained by calling 'f' on keys of 'in'.
func MapKeyWithError[T, K comparable, V any](in map[T]V, f func(T) (K, error)) (map[K]V, error) {
	out := make(map[K]V, len(in))
	for t, v := range in {
		k, err := f(t)
		if err != nil {
			return nil, err
		}
		out[k] = v
	}
	return out, nil
}

// HasAnyKey checks if any of the passed items is a key in the map.
// It returns false if nothing is passed.
// For multiple items it returns true if any of the items exist.
func HasAnyKey[T comparable, V any](s map[T]V, ts ...T) bool {
	has := false
	for _, t := range ts {
		if _, has = s[t]; has {
			break
		}
	}
	return has
}
