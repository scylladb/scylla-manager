// Copyright (C) 2017 ScyllaDB

package managerclient

import (
	"testing"

	"github.com/google/go-cmp/cmp"
)

func TestFormatError(t *testing.T) {
	t.Parallel()

	table := []struct {
		Name   string
		Msg    string
		Golden string
	}{
		{
			Name: "many_hosts_wrapped",
			Msg:  `create cluster: connectivity check failed: 192.168.100.13: unknown network bla; 192.168.100.22: unknown network bla; 192.168.100.12: unknown network bla; 192.168.100.23: unknown network bla; 192.168.100.11: unknown network bla; 192.168.100.21: unknown network bla`,
			Golden: `create cluster: connectivity check failed
 192.168.100.13: unknown network bla
 192.168.100.22: unknown network bla
 192.168.100.12: unknown network bla
 192.168.100.23: unknown network bla
 192.168.100.11: unknown network bla
 192.168.100.21: unknown network bla`,
		},
		{
			Name: "single_host_wrapped",
			Msg:  `create cluster: connectivity check failed: 192.168.100.13: unknown network bla`,
			Golden: `create cluster: connectivity check failed
 192.168.100.13: unknown network bla`,
		},
		{
			Name: "many_hosts_unwrapped",
			Msg:  `192.168.100.13: unknown network bla; 192.168.100.22: unknown network bla; 192.168.100.12: unknown network bla; 192.168.100.23: unknown network bla; 192.168.100.11: unknown network bla; 192.168.100.21: unknown network bla`,
			Golden: `
 192.168.100.13: unknown network bla
 192.168.100.22: unknown network bla
 192.168.100.12: unknown network bla
 192.168.100.23: unknown network bla
 192.168.100.11: unknown network bla
 192.168.100.21: unknown network bla`,
		},
		{
			Name: "single_host_unwrapped",
			Msg:  `192.168.100.13: unknown network bla`,
			Golden: `
 192.168.100.13: unknown network bla`,
		},
	}

	for i := range table {
		test := table[i]

		t.Run(test.Name, func(t *testing.T) {
			t.Parallel()
			if diff := cmp.Diff(FormatError(test.Msg), test.Golden); diff != "" {
				t.Fatal(diff)
			}
		})
	}
}

