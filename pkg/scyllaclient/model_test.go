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

func TestScyllaFeatures(t *testing.T) {
	t.Parallel()

	table := []struct {
		Version string
		Golden  ScyllaFeatures
	}{
		{
			Version: "2019.1.2-0.20190814.2772d52",
			Golden: ScyllaFeatures{
				RowLevelRepair: false,
			},
		},
		{
			Version: "3.1.0-0.20191012.9c3cdded9",
			Golden: ScyllaFeatures{
				RowLevelRepair: true,
			},
		},
		{
			Version: "3.2.2-0.20200222.0b23e7145d0",
			Golden: ScyllaFeatures{
				RowLevelRepair: true,
			},
		},
		{
			Version: scyllaMasterVersion,
			Golden: ScyllaFeatures{
				RowLevelRepair: true,
			},
		},
		{
			Version: scyllaEnterpriseMasterVersion,
			Golden: ScyllaFeatures{
				RowLevelRepair: true,
			},
		},
		{
			Version: "3.3.rc2",
			Golden: ScyllaFeatures{
				RowLevelRepair: true,
			},
		},
		{
			Version: "3.1.hotfix",
			Golden: ScyllaFeatures{
				RowLevelRepair: true,
			},
		},
		{
			Version: "3.0.rc8",
			Golden: ScyllaFeatures{
				RowLevelRepair: false,
			},
		},
		{
			Version: "2019.1.1-2.reader_concurrency_semaphore.20190730.f0071c669",
			Golden: ScyllaFeatures{
				RowLevelRepair: false,
			},
		},
		{
			Version: "2019.1.5-2.many_tables.20200311.be960ed96",
			Golden: ScyllaFeatures{
				RowLevelRepair: false,
			},
		},
	}

	for i := range table {
		test := table[i]

		f, err := makeScyllaFeatures(test.Version)
		if err != nil {
			t.Fatal("makeScyllaFeatures() error", err)
		}

		if diff := cmp.Diff(f, test.Golden); diff != "" {
			t.Errorf("makeScyllaFeatures(%s) = %+v, diff %s", test.Version, f, diff)
		}
	}
}
