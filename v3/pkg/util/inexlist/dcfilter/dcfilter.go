// Copyright (C) 2017 ScyllaDB

package dcfilter

import (
	"sort"

	"github.com/pkg/errors"
	"github.com/scylladb/scylla-manager/v3/pkg/util"
	"github.com/scylladb/scylla-manager/v3/pkg/util/inexlist"
)

// Apply returns DCs matched by the filters.
// If no DCs are found or a filter is invalid a validation error is returned.
func Apply(dcMap map[string][]string, filters []string) ([]string, error) {
	dcInclExcl, err := inexlist.ParseInExList(decorate(filters))
	if err != nil {
		return nil, err
	}

	dcs := make([]string, 0, len(dcMap))
	for dc := range dcMap {
		dcs = append(dcs, dc)
	}

	// Filter
	filtered := dcInclExcl.Filter(dcs)

	// Report error if there is no match
	if len(filtered) == 0 {
		return nil, util.ErrValidate(errors.Errorf("no matching DCs found for filters %s", filters))
	}

	// Sort lexicographically
	sort.Strings(filtered)

	return filtered, nil
}

// Filter that lets you filter datacenters.
type Filter struct {
	filters []string
	inex    inexlist.InExList
}

func NewFilter(filters []string) (*Filter, error) {
	// Decorate filters and create inexlist
	inex, err := inexlist.ParseInExList(decorate(filters))
	if err != nil {
		return nil, errors.Wrapf(err, "parse dc filter %v", filters)
	}
	return &Filter{
		filters: filters,
		inex:    inex,
	}, nil
}

// Check returns true iff dc matches filter.
func (f *Filter) Check(dc string) bool {
	return len(f.inex.Filter([]string{dc})) > 0
}

func decorate(filters []string) []string {
	if len(filters) == 0 {
		filters = append(filters, "*")
	}
	return filters
}
