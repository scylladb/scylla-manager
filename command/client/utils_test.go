// Copyright (C) 2017 ScyllaDB

package client

import (
	"encoding/json"
	"errors"
	"testing"

	"github.com/go-openapi/runtime"
	"github.com/google/go-cmp/cmp"
)

func TestErrorStr(t *testing.T) {
	table := []struct {
		E error
		S string
	}{
		{
			E: errors.New("foo"),
			S: "Error: foo",
		},
		{
			E: runtime.NewAPIError("foo", "bar", 418),
			S: "foo (status 418)",
		},
		{
			E: runtime.NewAPIError("foo", json.RawMessage("bar"), 418),
			S: "foo (status 418)\nbar",
		},
		{
			E: runtime.NewAPIError("foo", json.RawMessage(`"bar"`), 418),
			S: "foo (status 418)\n\"bar\"",
		},
	}

	for _, test := range table {
		if diff := cmp.Diff(errorStr(test.E), test.S); diff != "" {
			t.Error(diff)
		}
	}
}
