// Copyright (C) 2017 ScyllaDB

package ksfilter

import (
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	"github.com/scylladb/mermaid/internal/inexlist"
)

func TestValidate(t *testing.T) {
	table := []struct {
		F string
		E string
	}{
		// known invalid cases
		{
			F: ".*kalle.*",
			E: "missing keyspace",
		},
		{
			F: ".*",
			E: "missing keyspace",
		},
	}

	for i, test := range table {
		if err := validate(test.F); err == nil || err.Error() != test.E {
			t.Error(i, "got", err, "expected", test.E)
		}
	}
}

func TestDecorate(t *testing.T) {
	table := []struct {
		F []string
		E []string
	}{
		{
			F: []string{},
			E: []string{"*.*"},
		},
		{
			F: []string{"*"},
			E: []string{"*.*"},
		},
		{
			F: []string{"kalle"},
			E: []string{"kalle.*"},
		},
		{
			F: []string{"kalle*"},
			E: []string{"kalle*.*"},
		},
		{
			F: []string{"*kalle"},
			E: []string{"*kalle.*"},
		},
		{
			F: []string{"kalle.*"},
			E: []string{"kalle.*"},
		},
		{
			F: []string{"*kalle.*"},
			E: []string{"*kalle.*"},
		},
	}

	for i, test := range table {
		f := decorate(test.F)
		if !cmp.Equal(test.E, f, cmpopts.EquateEmpty()) {
			t.Error(i, "expected", test.E, "got", f)
		}
	}
}

func TestSortUnits(t *testing.T) {
	defaultTables := []string{"t"}

	var table = []struct {
		P []string
		U []string
		E []string
	}{
		// no patterns, promote system tables
		{
			P: nil,
			U: []string{"test_keyspace_dc1_rf2", "test_keyspace_dc2_rf3", "test_keyspace_rf2", "test_keyspace_rf3", "system_auth", "system_traces"},
			E: []string{"system_auth", "system_traces", "test_keyspace_dc1_rf2", "test_keyspace_dc2_rf3", "test_keyspace_rf2", "test_keyspace_rf3"},
		},
		// follow pattern order
		{
			P: []string{"test*", "system*"},
			U: []string{"test_keyspace_dc1_rf2", "test_keyspace_dc2_rf3", "test_keyspace_rf2", "test_keyspace_rf3", "system_auth", "system_traces"},
			E: []string{"test_keyspace_dc1_rf2", "test_keyspace_dc2_rf3", "test_keyspace_rf2", "test_keyspace_rf3", "system_auth", "system_traces"},
		},
		// follow pattern order
		{
			P: []string{"*dc2*", "system*", "*dc1*"},
			U: []string{"test_keyspace_dc1_rf2", "test_keyspace_dc2_rf3", "test_keyspace_rf2", "test_keyspace_rf3", "system_auth", "system_traces"},
			E: []string{"test_keyspace_dc2_rf3", "system_auth", "system_traces", "test_keyspace_dc1_rf2", "test_keyspace_rf2", "test_keyspace_rf3"},
		},
	}

	for i, test := range table {
		l, err := inexlist.ParseInExList(decorate(test.P))
		if err != nil {
			t.Fatal(err)
		}
		u := make([]Unit, len(test.U))
		for i := range test.U {
			u[i] = Unit{
				Keyspace: test.U[i],
				Tables:   defaultTables,
			}
		}
		sortUnits(u, l)
		e := make([]string, len(test.U))
		for i := range test.U {
			e[i] = u[i].Keyspace
		}
		if diff := cmp.Diff(e, test.E); diff != "" {
			t.Error(i, e)
		}
	}
}

func TestFilterAdd(t *testing.T) {
	filters := []string{
		"system",
		"*.*foo",
		"!bar.*foo",
	}

	f, err := NewFilter(filters)
	if err != nil {
		t.Fatal(err)
	}
	f.Add("bar", []string{"foo", "bar", "baz"})
	f.Add("baz", []string{"foo", "bar", "baz"})
	f.Add("system", []string{"foo", "bar", "baz"})

	units, err := f.Apply(false)
	if err != nil {
		t.Fatal(err)
	}

	expected := []Unit{
		{
			Keyspace:  "system",
			Tables:    []string{"foo", "bar", "baz"},
			AllTables: true,
		},
		{
			Keyspace:  "baz",
			Tables:    []string{"foo"},
			AllTables: false,
		},
	}

	if diff := cmp.Diff(units, expected); diff != "" {
		t.Fatal(diff)
	}
}
