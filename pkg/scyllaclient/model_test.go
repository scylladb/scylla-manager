// Copyright (C) 2017 ScyllaDB

package scyllaclient

import (
	"sort"
	"testing"

	"github.com/google/go-cmp/cmp"
)

func TestRingDatacenters(t *testing.T) {
	r := Ring{
		HostDC: map[string]string{
			"172.16.1.10": "dc1",
			"172.16.1.2":  "dc1",
			"172.16.1.20": "dc2",
			"172.16.1.3":  "dc1",
			"172.16.1.4":  "dc2",
			"172.16.1.5":  "dc2",
		},
	}
	d := r.Datacenters()
	sort.Strings(d)
	if diff := cmp.Diff(d, []string{"dc1", "dc2"}); diff != "" {
		t.Fatal(diff)
	}
}
