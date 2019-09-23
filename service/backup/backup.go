// Copyright (C) 2017 ScyllaDB

package backup

import (
	"strings"

	"github.com/pkg/errors"
	"github.com/scylladb/go-set/strset"
	"go.uber.org/multierr"
)

func dcHosts(dcMap map[string][]string, dcs []string) []string {
	var hosts []string
	for _, dc := range dcs {
		h := dcMap[dc]
		hosts = append(hosts, h...)
	}
	return hosts
}

func checkDCs(dcAtPos func(int) (string, string), n int, dcMap map[string][]string) (err error) {
	allDCs := strset.New()
	for dc := range dcMap {
		allDCs.Add(dc)
	}

	for i := 0; i < n; i++ {
		dc, str := dcAtPos(i)
		if dc == "" {
			continue
		}
		if !allDCs.Has(dc) {
			err = multierr.Append(err, errors.Errorf("no such datacenter %s in %s", dc, str))
		}
	}
	return
}

func checkAllDCsCovered(dcAtPos func(int) string, n int, dcs []string) error {
	hasDCs := strset.New()
	hasDefault := false

	for i := 0; i < n; i++ {
		dc := dcAtPos(i)
		if dc == "" {
			hasDefault = true
			continue
		}
		hasDCs.Add(dc)
	}

	if !hasDefault {
		if d := strset.Difference(strset.New(dcs...), hasDCs); !d.IsEmpty() {
			return errors.Errorf("missing configurations for datacenters %s", strings.Join(d.List(), " "))
		}
	}

	return nil
}

func hostInfoFromHosts(hosts []string, dcMap map[string][]string, hostIDs map[string]string, locations []Location, rateLimits []DCLimit) ([]hostInfo, error) {
	// Host to DC index
	hdc := map[string]string{}
	for dc, hosts := range dcMap {
		for _, h := range hosts {
			hdc[h] = dc
		}
	}

	// DC location index
	dcl := map[string]Location{}
	for _, l := range locations {
		dcl[l.DC] = l
	}

	// DC rate limit index
	dcr := map[string]DCLimit{}
	for _, r := range rateLimits {
		dcr[r.DC] = r
	}

	var (
		hi   = make([]hostInfo, len(hosts))
		errs error
	)
	for i, h := range hosts {
		var ok bool

		dc, ok := hdc[h]
		if !ok {
			errs = multierr.Append(errs, errors.Errorf("%s: unknown datacenter", h))
		}
		hi[i].DC = dc
		hi[i].IP = h
		hi[i].ID, ok = hostIDs[h]
		if !ok {
			errs = multierr.Append(errs, errors.Errorf("%s: unknown ID", h))
		}
		hi[i].Location, ok = dcl[dc]
		if !ok {
			hi[i].Location, ok = dcl[""]
			if !ok {
				errs = multierr.Append(errs, errors.Errorf("%s: unknown location", h))
			}
		}
		hi[i].RateLimit, ok = dcr[dc]
		if !ok {
			hi[i].RateLimit = dcr[""] // no rate limit is ok, fallback to 0 - no limit
		}
	}

	return hi, errs
}

// sliceContains returns true if str can be found in provided items.
func sliceContains(str string, items []string) bool {
	for _, i := range items {
		if i == str {
			return true
		}
	}
	return false
}

// filterDCLocations takes list of locations and returns only locations that
// belong the provided list of data centers.
func filterDCLocations(locations []Location, dcs []string) []Location {
	var filtered []Location
	for _, l := range locations {
		if l.DC == "" || sliceContains(l.DC, dcs) {
			filtered = append(filtered, l)
			continue
		}
	}
	return filtered
}

// filterDCLimits takes list of DCLimits and returns only locations that belong
// the provided list of data centers.
func filterDCLimits(limits []DCLimit, dcs []string) []DCLimit {
	var filtered []DCLimit
	for _, l := range limits {
		if l.DC == "" || sliceContains(l.DC, dcs) {
			filtered = append(filtered, l)
			continue
		}
	}
	return filtered
}
