// Copyright (C) 2017 ScyllaDB

package backup

import (
	"github.com/pkg/errors"
	"github.com/scylladb/mermaid/pkg/util/parallel"
	"go.uber.org/multierr"
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
	for _, h := range hosts {
		dc := h.DC
		// If DC has no limit put host under an empty DC
		if _, ok := dcLimit[dc]; !ok {
			dc = ""
		}

		v, ok := m[dc]
		if !ok {
			v = hostsLimit{}
			v.hosts = []hostInfo{h}
			if dc == "" {
				v.limit = globalLimit
			} else {
				v.limit = dcLimit[dc]
			}
		} else {
			v.hosts = append(v.hosts, h)
		}
		m[dc] = v
	}

	return m
}

func inParallelWithLimits(hosts []hostInfo, limits []DCLimit, f func(h hostInfo) error) error {
	m := makeHostsLimit(hosts, limits)

	out := make(chan error)
	for _, hl := range m {
		go func(hl hostsLimit) {
			out <- hostsInParallel(hl.hosts, hl.limit, f)
		}(hl)
	}

	var errs error
	for range m {
		errs = multierr.Append(errs, <-out)
	}
	return errs
}

func hostsInParallel(hosts []hostInfo, limit int, f func(h hostInfo) error) error {
	return parallel.Run(len(hosts), limit, func(i int) error {
		return errors.Wrapf(f(hosts[i]), "%s", hosts[i])
	})
}

const dirsInParallelLimit = 10

func dirsInParallel(dirs []snapshotDir, abortOnError bool, f func(h snapshotDir) error) error {
	return parallel.Run(len(dirs), dirsInParallelLimit, func(i int) error {
		if err := errors.Wrapf(f(dirs[i]), "%s", dirs[i]); err != nil {
			if abortOnError {
				return parallel.Abort(err)
			}
			return err
		}
		return nil
	})
}
