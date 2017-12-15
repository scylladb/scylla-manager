// Copyright (C) 2017 ScyllaDB

package cluster

import (
	"context"
	"fmt"
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/pkg/errors"
)

func TestServiceValidateHosts(t *testing.T) {
	type res struct {
		C  string
		DC string
		E  error
	}

	table := []struct {
		H []string
		R map[string]res
		E string
	}{
		{
			// Error
			H: []string{"a", "b"},
			R: map[string]res{"a": {E: errors.New("error")}, "b": {C: "a", DC: "b"}},
			E: "error",
		},
		{
			// Cluster mixup
			H: []string{"a", "b"},
			R: map[string]res{"a": {C: "a", DC: "b"}, "b": {C: "b", DC: "b"}},
			E: "mixed clusters",
		},
		{
			// DC mixup
			H: []string{"a", "b"},
			R: map[string]res{"a": {C: "a", DC: "b"}, "b": {C: "a", DC: "a"}},
			E: "mixed datacenters",
		},
		{
			// OK
			H: []string{"a", "b"},
			R: map[string]res{"a": {C: "a", DC: "b"}, "b": {C: "a", DC: "b"}},
		},
	}

	for i, test := range table {
		s := Service{hostCluterDC: func(_ context.Context, host string) (cluster, dc string, err error) {
			v, ok := test.R[host]
			if !ok {
				t.Fatal(i, host)
			}
			return v.C, v.DC, v.E
		}}

		if test.E == "" {
			test.E = "<nil>"
		}
		if diff := cmp.Diff(fmt.Sprint(s.validateHosts(context.Background(), test.H)), test.E); diff != "" {
			t.Error(i, diff)
		}
	}
}
