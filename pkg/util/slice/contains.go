// Copyright (C) 2017 ScyllaDB

package slice

// Contains is a general purpose function to check if a slice contains element.
// It has a linear complexity, and does not assume any structure of data.
// Most likely you want to use one of the typed functions `ContainsX` from this
// package instead of this function.
func Contains(n int, match func(i int) bool) bool {
	for i := 0; i < n; i++ {
		if match(i) {
			return true
		}
	}
	return false
}

// ContainsString returns true iff one of elements of a is s.
func ContainsString(a []string, s string) bool {
	return Contains(len(a), func(i int) bool {
		return a[i] == s
	})
}
