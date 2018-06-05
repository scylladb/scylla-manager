// Copyright (C) 2017 ScyllaDB

package main

import (
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/scylladb/mermaid/uuid"
)

func TestTaskSplit(t *testing.T) {
	table := []struct {
		S  string
		T  string
		ID uuid.UUID
	}{
		{
			S:  "repair/d7d4b241-f7fe-434e-bc8e-6185b30b078a",
			T:  "repair",
			ID: uuid.MustParse("d7d4b241-f7fe-434e-bc8e-6185b30b078a"),
		},
		{
			S:  "d7d4b241-f7fe-434e-bc8e-6185b30b078a",
			ID: uuid.MustParse("d7d4b241-f7fe-434e-bc8e-6185b30b078a"),
		},
	}

	for i, test := range table {
		tp, id, err := taskSplit(test.S)
		if err != nil {
			t.Error(i, err)
		}
		if tp != test.T {
			t.Error(i, tp)
		}
		if id != test.ID {
			t.Error(i, id)
		}
	}
}

func TestDumpMap(t *testing.T) {
	table := []struct {
		M map[string]interface{}
		S string
	}{
		// Empty
		{
			S: "-",
		},
		// Single element
		{
			M: map[string]interface{}{"a": "b"},
			S: "a:b",
		},
		// Multiple elements
		{
			M: map[string]interface{}{"a": "b", "c": "d"},
			S: "a:b, c:d",
		},
	}

	for i, test := range table {
		if diff := cmp.Diff(dumpMap(test.M), test.S); diff != "" {
			t.Error(i, diff)
		}
	}
}
