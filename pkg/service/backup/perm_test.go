// Copyright (C) 2017 ScyllaDB

package backup

import (
	"sort"
	"testing"
)

func TestUnitsPerm(t *testing.T) {
	t.Parallel()

	table := []struct {
		Name  string
		Units []Unit
	}{
		{
			Name: "Empty",
		},
		{
			Name:  "One unit, no schema",
			Units: []Unit{{Keyspace: "a"}},
		},
		{
			Name:  "One unit just schema",
			Units: []Unit{{Keyspace: systemSchema}},
		},
		{
			Name:  "More units, no schema",
			Units: []Unit{{Keyspace: "a"}, {Keyspace: "b"}, {Keyspace: "c"}},
		},
		{
			Name:  "More units, with schema",
			Units: []Unit{{Keyspace: systemSchema}, {Keyspace: "a"}, {Keyspace: "b"}, {Keyspace: "c"}},
		},
	}

	for i := range table {
		test := table[i]
		t.Run(test.Name, func(t *testing.T) {
			perm := unitsPerm(test.Units)

			systemSchemaPos := -1
			for i, u := range test.Units {
				if u.Keyspace == systemSchema {
					systemSchemaPos = i
					break
				}
			}
			if systemSchemaPos >= 0 && perm[len(perm)-1] != systemSchemaPos {
				t.Fatal("Expected system schema as last item")
			}
			sort.Ints(perm)
			for i, v := range perm {
				if i != v {
					t.Fatal("Not a permutation")
				}
			}
		})
	}
}
