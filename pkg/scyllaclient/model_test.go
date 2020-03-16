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

func TestScyllaFeatures(t *testing.T) {
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
