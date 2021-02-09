// Copyright (C) 2017 ScyllaDB

package backup

import (
	"encoding/json"
	"strings"

	"github.com/pkg/errors"
	"github.com/scylladb/go-set/strset"
	"github.com/scylladb/scylla-manager/pkg/backup"
	"github.com/scylladb/scylla-manager/pkg/scyllaclient"
	"go.uber.org/multierr"
)

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
			err = multierr.Append(err, errors.Errorf("%q no such datacenter %s", str, dc))
		}
	}
	return
}

func checkAllDCsCovered(locations []backup.Location, dcs []string) error {
	hasDCs := strset.New()
	hasDefault := false

	for _, l := range locations {
		if l.DC == "" {
			hasDefault = true
			continue
		}
		hasDCs.Add(l.DC)
	}

	if !hasDefault {
		if d := strset.Difference(strset.New(dcs...), hasDCs); !d.IsEmpty() {
			msg := "missing location(s) for datacenters %s"
			if d.Size() == 1 {
				msg = "missing location for datacenter %s"
			}
			return errors.Errorf(msg, strings.Join(d.List(), ", "))
		}
	}

	return nil
}

func makeHostInfo(nodes []scyllaclient.NodeStatusInfo, locations []backup.Location, rateLimits []DCLimit) ([]hostInfo, error) {
	// DC location index
	dcl := map[string]backup.Location{}
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
// belong to the provided list of datacenters.
func filterDCLocations(locations []backup.Location, dcs []string) []backup.Location {
	var filtered []backup.Location
	for _, l := range locations {
		if l.DC == "" || sliceContains(l.DC, dcs) {
			filtered = append(filtered, l)
			continue
		}
	}
	return filtered
}

// filterDCLimits takes list of DCLimits and returns only locations that belong
// to the provided list of datacenters.
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

func extractLocations(properties []json.RawMessage) ([]backup.Location, error) {
	var (
		m         = strset.New()
		locations []backup.Location
		errs      error
	)

	p := struct {
		Location []backup.Location `json:"location"`
	}{}
	for i := range properties {
		if err := json.Unmarshal(properties[i], &p); err != nil {
			errs = multierr.Append(
				errs,
				errors.Wrapf(err, "parse runner properties: %s", properties),
			)
			continue
		}
		// Add location once
		for _, l := range p.Location {
			if key := l.RemotePath(""); !m.Has(key) {
				m.Add(key)
				locations = append(locations, l)
			}
		}
		// Clear locations
		p.Location = nil
	}

	return locations, errs
}
