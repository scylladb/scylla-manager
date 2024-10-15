// Copyright (C) 2017 ScyllaDB

package backup

import (
	"testing"

	"github.com/google/go-cmp/cmp"
	. "github.com/scylladb/scylla-manager/v3/pkg/service/backup/backupspec"
)

func TestMakeHostsLimit(t *testing.T) {
	t.Parallel()

	var (
		dc1h1 = hostInfo{DC: "dc1", ID: "1"}
		dc1h2 = hostInfo{DC: "dc1", ID: "2"}
		dc2h1 = hostInfo{DC: "dc2", ID: "1"}
		dc2h2 = hostInfo{DC: "dc2", ID: "2"}

		limits = []DCLimit{{DC: "dc1", Limit: 10}, {DC: "", Limit: 100}}
	)

	table := []struct {
		Name       string
		Hosts      []hostInfo
		Limits     []DCLimit
		HostsLimit map[string]hostsLimit
	}{
		{
			Name:       "dc limit",
			Hosts:      []hostInfo{dc1h1, dc1h2},
			Limits:     limits,
			HostsLimit: map[string]hostsLimit{"dc1": {hosts: []hostInfo{dc1h1, dc1h2}, limit: 10}},
		},
		{
			Name:       "global limit",
			Hosts:      []hostInfo{dc2h1, dc2h2},
			Limits:     limits,
			HostsLimit: map[string]hostsLimit{"": {hosts: []hostInfo{dc2h1, dc2h2}, limit: 100}},
		},
		{
			Name:  "no limit",
			Hosts: []hostInfo{dc1h1, dc1h2, dc2h1, dc2h2},
			HostsLimit: map[string]hostsLimit{
				"": {hosts: []hostInfo{dc1h1, dc1h2, dc2h1, dc2h2}},
			},
		},
		{
			Name:   "mixed limit",
			Hosts:  []hostInfo{dc1h1, dc1h2, dc2h1, dc2h2},
			Limits: limits,
			HostsLimit: map[string]hostsLimit{
				"dc1": {hosts: []hostInfo{dc1h1, dc1h2}, limit: 10},
				"":    {hosts: []hostInfo{dc2h1, dc2h2}, limit: 100},
			},
		},
	}

	for i := range table {
		test := table[i]

		t.Run(test.Name, func(t *testing.T) {
			t.Parallel()

			golden := test.HostsLimit
			m := makeHostsLimit(test.Hosts, test.Limits)
			if diff := cmp.Diff(golden, m, cmp.AllowUnexported(hostsLimit{})); diff != "" {
				t.Errorf(diff)
			}
		})
	}
}
