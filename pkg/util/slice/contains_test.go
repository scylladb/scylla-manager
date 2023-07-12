// Copyright (C) 2017 ScyllaDB

package slice

import "testing"

func TestContains(t *testing.T) {
	t.Parallel()

	t.Run("string", func(t *testing.T) {
		t.Parallel()

		check := func(a []string, s string, golden bool) {
			if v := ContainsString(a, s); v != golden {
				t.Errorf("ContainsString(%v, %s) = %v, expected %v", a, s, v, golden)
			}
		}

		a := []string{"a", "b", "c"}
		check(a, "a", true)
		check(a, "b", true)
		check(a, "d", false)

		check(nil, "a", false)
	})
}
