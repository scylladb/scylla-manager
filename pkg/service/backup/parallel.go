// Copyright (C) 2017 ScyllaDB

package backup

import (
	"github.com/pkg/errors"
	. "github.com/scylladb/scylla-manager/v3/pkg/service/backup/backupspec"
	"github.com/scylladb/scylla-manager/v3/pkg/util/parallel"
)

type hostsLimit struct {
	hosts []hostInfo
	limit int
}

// makeHostsLimit returns a mapping from datacenter name to list of hosts in
// that datacenter and a limit applicable for that datacenter.
// It groups hosts in the same datacenter for datacenters specified
// in limits. Hosts from datacenters not specified in limits are joined together
// and kept under "" key in the resulting map.
func makeHostsLimit(hosts []hostInfo, limits []DCLimit) map[string]hostsLimit {
	// DC limit index
	var (
		dcLimit     = map[string]int{}
		globalLimit = 0
	)
	for _, l := range limits {
		if l.DC != "" {
			dcLimit[l.DC] = l.Limit
		} else {
			globalLimit = l.Limit
		}
	}

	m := make(map[string]hostsLimit, len(dcLimit)+1)
	for i := range hosts {
		dc := hosts[i].DC
		// If DC has no limit put host under an empty DC
		if _, ok := dcLimit[dc]; !ok {
			dc = ""
		}

		v, ok := m[dc]
		if !ok {
			v = hostsLimit{}
			v.hosts = []hostInfo{hosts[i]}
			if dc == "" {
				v.limit = globalLimit
			} else {
				v.limit = dcLimit[dc]
			}
		} else {
			v.hosts = append(v.hosts, hosts[i])
		}
		m[dc] = v
	}

	return m
}

func inParallelWithLimits(hosts []hostInfo, limits []DCLimit, f func(h hostInfo) error, notify func(h hostInfo, err error)) error {
	m := makeHostsLimit(hosts, limits)

	out := make(chan error)
	for _, hl := range m {
		go func(hl hostsLimit) {
			out <- hostsInParallel(hl.hosts, hl.limit, f, notify)
		}(hl)
	}

	var retErr error
	for range m {
		err := <-out
		// Appending all errors reduces readability, so it's enough to return only one of them
		// (the other errors has already been logged via notify).
		if retErr == nil {
			retErr = err
		}
	}
	return retErr
}

func hostsInParallel(hosts []hostInfo, limit int, f func(h hostInfo) error, notify func(h hostInfo, err error)) error {
	return parallel.Run(len(hosts), limit,
		func(i int) error {
			return errors.Wrapf(f(hosts[i]), "%s", hosts[i])
		},
		func(i int, err error) {
			notify(hosts[i], err)
		})
}
