// Copyright (C) 2017 ScyllaDB

package uuid

import "testing"

func TestCompare(t *testing.T) {
	t.Parallel()

	t.Run("eq", func(t *testing.T) {
		t.Parallel()

		u := MustRandom()
		if Compare(u, u) != 0 {
			t.Fatal()
		}
	})

	t.Run("timeuuid lt", func(t *testing.T) {
		t.Parallel()

		t0 := NewTime()
		t1 := NewTime()
		if Compare(t0, t1) != -1 {
			t.Fatal()
		}
	})

	t.Run("timeuuid gt", func(t *testing.T) {
		t.Parallel()

		t0 := NewTime()
		t1 := NewTime()
		if Compare(t1, t0) != 1 {
			t.Fatal()
		}
	})
}
