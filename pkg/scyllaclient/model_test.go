// Copyright (C) 2017 ScyllaDB

package scyllaclient

import (
	"sort"
	"testing"

	"github.com/google/go-cmp/cmp"
)

func TestNodeStatusInfo(t *testing.T) {
	t.Parallel()

	test := NodeStatusInfoSlice{
		{Datacenter: "dc1", Addr: "192.168.100.11", State: NodeStateJoining, Status: NodeStatusUp},
		{Datacenter: "dc1", Addr: "192.168.100.12", State: NodeStateLeaving, Status: NodeStatusUp},
		{Datacenter: "dc1", Addr: "192.168.100.13", State: "", Status: NodeStatusDown},
		{Datacenter: "dc2", Addr: "192.168.100.21", State: NodeStateMoving, Status: NodeStatusUp},
		{Datacenter: "dc2", Addr: "192.168.100.22", State: "", Status: NodeStatusUp},
		{Datacenter: "dc2", Addr: "192.168.100.23", State: "", Status: NodeStatusDown},
	}

	if v := test.Datacenter(nil); len(v) != 0 {
		t.Errorf("Datacenter(dc1) = %+v, expected length 0", v)
	}
	if diff := cmp.Diff(test.Datacenter([]string{"dc1"}), test[0:3]); diff != "" {
		t.Errorf("Datacenter(dc1) = %+v, diff %s", test.Datacenter([]string{"dc1"}), diff)
	}
	if diff := cmp.Diff(test.Hosts(), []string{
		"192.168.100.11", "192.168.100.12", "192.168.100.13", "192.168.100.21", "192.168.100.22", "192.168.100.23",
	}); diff != "" {
		t.Errorf("Hosts() = %+v, diff %s", test.Hosts(), diff)
	}
	if diff := cmp.Diff(test.LiveHosts(), []string{"192.168.100.22"}); diff != "" {
		t.Errorf("LiveHosts() = %+v, diff %s", test.LiveHosts(), diff)
	}
	if diff := cmp.Diff(test.DownHosts(), []string{"192.168.100.13", "192.168.100.23"}); diff != "" {
		t.Errorf("DownHosts() = %+v, diff %s", test.DownHosts(), diff)
	}
}

func TestRingDatacenters(t *testing.T) {
	t.Parallel()

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
