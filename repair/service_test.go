// Copyright (C) 2017 ScyllaDB

package repair

import (
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/scylladb/mermaid/internal/inexlist"
)

func TestSortUnits(t *testing.T) {
	var table = []struct {
		P []string
		U []Unit
		E []Unit
	}{
		{
			P: []string{"keyspace"},
			U: []Unit{
				{
					Keyspace: "keyspace1",
					Tables:   []string{"kalle", "arne"},
				},
				{
					Keyspace: "keyspace",
					Tables:   []string{"kalle", "arne"},
				},
			},
			E: []Unit{
				{
					Keyspace: "keyspace",
					Tables:   []string{"kalle", "arne"},
				},
				{
					Keyspace: "keyspace1",
					Tables:   []string{"kalle", "arne"},
				},
			},
		},
		{
			P: []string{"!*"},
			U: []Unit{
				{
					Keyspace: "keyspace1",
					Tables:   []string{"kalle", "arne"},
				},
				{
					Keyspace: "keyspace",
					Tables:   []string{"kalle", "arne"},
				},
			},
			E: []Unit{
				{
					Keyspace: "keyspace",
					Tables:   []string{"kalle", "arne"},
				},
				{
					Keyspace: "keyspace1",
					Tables:   []string{"kalle", "arne"},
				},
			},
		},
		{
			P: []string{"keyspace", "!arne"},
			U: []Unit{
				{
					Keyspace: "keyspace1",
					Tables:   []string{"kalle", "arne"},
				},
				{
					Keyspace: "other",
					Tables:   []string{"kalle", "arne"},
				},
				{
					Keyspace: "keyspace",
					Tables:   []string{"kalle", "arne"},
				},
			},
			E: []Unit{
				{
					Keyspace: "keyspace",
					Tables:   []string{"kalle", "arne"},
				},
				{
					Keyspace: "other",
					Tables:   []string{"kalle", "arne"},
				},
				{
					Keyspace: "keyspace1",
					Tables:   []string{"kalle", "arne"},
				},
			},
		},
		{
			P: []string{"!arne", "keyspace"},
			U: []Unit{
				{
					Keyspace: "keyspace1",
					Tables:   []string{"kalle", "arne"},
				},
				{
					Keyspace: "arne",
					Tables:   []string{"kalle", "arne"},
				},
				{
					Keyspace: "keyspace",
					Tables:   []string{"kalle", "arne"},
				},
			},
			E: []Unit{
				{
					Keyspace: "keyspace",
					Tables:   []string{"kalle", "arne"},
				},
				{
					Keyspace: "arne",
					Tables:   []string{"kalle", "arne"},
				},
				{
					Keyspace: "keyspace1",
					Tables:   []string{"kalle", "arne"},
				},
			},
		},
	}

	for i, test := range table {
		inclExcl, _ := inexlist.ParseInExList(test.P)

		oldUnits := make([]Unit, len(test.U))
		copy(oldUnits, test.U)
		sortUnits(test.U, inclExcl)
		if !cmp.Equal(test.E, test.U) {
			t.Errorf("position %d, pattern %v, expected %v, got %v", i, test.P, test.E, test.U)
		}
	}
}
