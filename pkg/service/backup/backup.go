// Copyright (C) 2017 ScyllaDB

package backup

import (
	"github.com/pkg/errors"
	"github.com/scylladb/scylla-manager/v3/pkg/scyllaclient"
	"github.com/scylladb/scylla-manager/v3/pkg/util/backupmanifest"
	"go.uber.org/multierr"
)

func makeHostInfo(nodes []scyllaclient.NodeStatusInfo, locations []backupmanifest.Location, rateLimits []DCLimit, transfers int) ([]hostInfo, error) {
	// DC location index
	dcl := map[string]backupmanifest.Location{}
	for _, l := range locations {
		dcl[l.DC] = l
	}

	// DC rate limit index
	dcr := map[string]DCLimit{}
	for _, r := range rateLimits {
		dcr[r.DC] = r
	}

	var (
		hi   = make([]hostInfo, len(nodes))
		errs error
	)
	for i, h := range nodes {
		var ok bool

		hi[i].DC = h.Datacenter
		hi[i].IP = h.Addr
		hi[i].ID = h.HostID
		hi[i].Location, ok = dcl[h.Datacenter]
		if !ok {
			hi[i].Location, ok = dcl[""]
			if !ok {
				errs = multierr.Append(errs, errors.Errorf("%s: unknown location", h))
			}
		}
		hi[i].RateLimit, ok = dcr[h.Datacenter]
		if !ok {
			hi[i].RateLimit = dcr[""] // no rate limit is ok, fallback to 0 - no limit
		}
		hi[i].Transfers = transfers
	}

	return hi, errs
}
