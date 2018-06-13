// Copyright (C) 2017 ScyllaDB

package mermaid

import (
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/pkg/errors"
)

func TestErrValidate(t *testing.T) {
	table := []struct {
		E error
		M string
		S string
	}{
		{},
		{
			E: errors.New("cause"),
			M: "msg",
			S: "msg: cause",
		},
		{
			E: errors.New("cause"),
			S: "cause",
		},
	}

	for i, test := range table {
		s := ""

		err := ErrValidate(test.E, test.M)
		if err != nil {
			s = err.Error()
		}

		if diff := cmp.Diff(s, test.S); diff != "" {
			t.Error(i, diff)
		}
	}
}
