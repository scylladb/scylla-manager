// Copyright (C) 2017 ScyllaDB

package inexlist

import (
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
)

func TestParseInExList(t *testing.T) {
	var table = []struct {
		P []string
		E InExList
	}{
		{
			P: []string{"keyspace"},
			E: InExList{
				patterns: []string{"keyspace"},
				list: []signedPattern{
					{
						Sign:    true,
						Pattern: "keyspace",
					},
				},
			},
		},
		{
			P: []string{"keyspace", "!keyspace.peers"},
			E: InExList{
				patterns: []string{"keyspace", "!keyspace.peers"},
				list: []signedPattern{
					{
						Sign:    true,
						Pattern: "keyspace",
					},
					{
						Sign:    false,
						Pattern: "keyspace.peers",
					},
				},
			},
		},
		{
			P: []string{"keyspace", "!keyspace.peers", "!somespace.some*", "somespace.other*"},
			E: InExList{
				patterns: []string{"keyspace", "!keyspace.peers", "!somespace.some*", "somespace.other*"},
				list: []signedPattern{
					{
						Sign:    true,
						Pattern: "keyspace",
					},
					{
						Sign:    false,
						Pattern: "keyspace.peers",
					},
					{
						Sign:    false,
						Pattern: "somespace.some*",
					},
					{
						Sign:    true,
						Pattern: "somespace.other*",
					},
				},
			},
		},
	}

	opt := cmpopts.IgnoreUnexported(signedPattern{})

	for i, test := range table {
		inEx, err := ParseInExList(test.P)
		if err != nil {
			t.Errorf("pos %d, malformed pattern %s", i, test.P)
		}

		if !cmp.Equal(inEx.list, test.E.list, opt) {
			t.Errorf("pos %d, expected %v, got=%v", i, test.E.list, inEx.list)
		}

		if !cmp.Equal(test.E.patterns, inEx.patterns, opt) {
			t.Errorf("pos %d, wrong pattern expected %s, got %s", i, test.P, inEx.patterns)
		}
		for i, v := range inEx.list {
			expected := test.E.list[i]

			if v.Sign != expected.Sign || v.Pattern != expected.Pattern {
				t.Errorf("pos %d, expected %s, got %s", i, expected, v)
			}
		}
	}
}

func TestInExListFilter(t *testing.T) {
	var table = []struct {
		P   []string
		I   []string
		out []string
	}{
		{
			P:   nil,
			I:   []string{"keyspace.kalle", "keyspace.pelle", "kalle.keyspace", "some.other"},
			out: []string{"keyspace.kalle", "keyspace.pelle", "kalle.keyspace", "some.other"},
		},
		{
			P:   []string{},
			I:   []string{"keyspace.kalle", "keyspace.pelle", "kalle.keyspace", "some.other"},
			out: []string{"keyspace.kalle", "keyspace.pelle", "kalle.keyspace", "some.other"},
		},
		{
			P:   []string{"keyspace.*", "!keyspace.pelle"},
			I:   []string{"keyspace.kalle", "keyspace.pelle", "kalle.keyspace", "some.other"},
			out: []string{"keyspace.kalle"},
		},
		{
			P:   []string{"keyspace.*"},
			I:   []string{"keyspace.kalle", "keyspace.pelle", "kalle.keyspace", "some.other"},
			out: []string{"keyspace.kalle", "keyspace.pelle"},
		},
		{
			P:   []string{"kalle.*"},
			I:   []string{"keyspace.kalle", "keyspace.pelle", "kalle.keyspace", "some.other"},
			out: []string{"kalle.keyspace"},
		},
		{
			P:   []string{"kalle", "some"},
			I:   []string{"keyspace.kalle", "keyspace.pelle", "kalle.keyspace", "some.other"},
			out: []string{},
		},
		{
			P:   []string{"keyspace.*"},
			I:   []string{"keyspace.kalle", "kalle.keyspace", "some.other"},
			out: []string{"keyspace.kalle"},
		},
		{
			P:   []string{"keyspace.**"},
			I:   []string{"keyspace.kalle", "kalle.keyspace", "some.other"},
			out: []string{"keyspace.kalle"},
		},
		{
			P:   []string{"!keyspace.*"},
			I:   []string{"keyspace.kalle", "kalle.keyspace", "some.other"},
			out: []string{},
		},
		{
			P:   []string{"!keyspace.*", "keyspace.kalle"},
			I:   []string{"keyspace.kalle", "keyspace.pelle", "kalle.keyspace", "some.other"},
			out: []string{"keyspace.kalle"},
		},
		{
			P:   []string{"!keyspace.*", "keyspace.kalle", "!kalle.*", "kalle.keyspace"},
			I:   []string{"keyspace.kalle", "keyspace.pelle", "kalle.keyspace", "some.other"},
			out: []string{"keyspace.kalle", "kalle.keyspace"},
		},
		{
			P:   []string{"!keyspace§.*", "keyspace.kalle", "!kalle.other", "kalle.keyspace"},
			I:   []string{"keyspace.kalle", "keyspace.pelle", "kalle.keyspace", "kalle.other", "some.other"},
			out: []string{"keyspace.kalle", "kalle.keyspace"},
		},
		{
			P:   []string{"*.kalle"},
			I:   []string{"keyspace.kalle", "keyspace.pelle", "kalle.keyspace", "some.kalle"},
			out: []string{"keyspace.kalle", "some.kalle"},
		},
		{
			P:   []string{"*"},
			I:   []string{"keyspace.kalle", "keyspace.pelle", "kalle.keyspace", "some.kalle"},
			out: []string{"keyspace.kalle", "keyspace.pelle", "kalle.keyspace", "some.kalle"},
		},
	}

	for i, test := range table {
		l, err := ParseInExList(test.P)
		if err != nil {
			t.Errorf("malformed pattern %s", test.P)
		}
		out := l.Filter(test.I)
		if !cmp.Equal(out, test.out, cmpopts.EquateEmpty()) {
			t.Errorf("pos %d, filtered list wrong expected %v, got %v", i, test.out, out)
		}
	}
}
