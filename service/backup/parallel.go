// Copyright (C) 2017 ScyllaDB

package backup

import (
	"github.com/pkg/errors"
	"go.uber.org/atomic"
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
			out <- inParallel(hl.hosts, hl.limit, f)
		}(hl)
	}

	var errs error
	for range m {
		errs = multierr.Append(errs, <-out)
	}
	return errs
}

func inParallel(hosts []hostInfo, limit int, f func(h hostInfo) error) error {
	if limit <= 0 {
		limit = len(hosts)
	}

	idx := atomic.NewInt32(0)
	out := make(chan error)
	for j := 0; j < limit; j++ {
		go func() {
			for {
				i := int(idx.Inc()) - 1
				if i >= len(hosts) {
					return
				}
				out <- errors.Wrapf(f(hosts[i]), "%s", hosts[i])
			}
		}()
	}

	var errs error
	for range hosts {
		errs = multierr.Append(errs, <-out)
	}
	return errs
}
