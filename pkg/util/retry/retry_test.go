// Copyright (C) 2017 ScyllaDB

package retry

import (
	"testing"

	"github.com/pkg/errors"
)

func TestPermanent(t *testing.T) {
	table := []struct {
		Name string
		Err  error
	}{
		{
			Name: "direct",
			Err:  Permanent(errors.New("test error")),
		},
		{
			Name: "wrapped",
			Err:  errors.Wrap(Permanent(errors.New("test error")), "foo"),
		},
	}

	for i := range table {
		test := table[i]
		t.Run(test.Name, func(t *testing.T) {
			if !IsPermanent(test.Err) {
				t.Fatal("Not permanent")
			}
		})
	}
}
