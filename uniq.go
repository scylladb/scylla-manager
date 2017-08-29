package mermaid

import "sort"

var keyExists = struct{}{}

// Uniq allows for string deduplication.
type Uniq map[string]struct{}

// Put records a string.
func (u Uniq) Put(s string) {
	u[s] = keyExists
}

// Has check if s is already present
func (u Uniq) Has(s string) bool {
	_, ok := u[s]
	return ok
}

// Slice dumps all values to string.
func (u Uniq) Slice() []string {
	if len(u) == 0 {
		return nil
	}

	s := make([]string, 0, len(u))
	for k := range u {
		s = append(s, k)
	}

	// make function pure
	sort.Strings(s)

	return s
}
