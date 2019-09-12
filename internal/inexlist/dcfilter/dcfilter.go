// Copyright (C) 2017 ScyllaDB

package dcfilter

import (
	"sort"

	"github.com/pkg/errors"
	"github.com/scylladb/mermaid"
	"github.com/scylladb/mermaid/internal/inexlist"
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
		return nil, mermaid.ErrValidate(errors.Errorf("no matching DCs found for filters %s", filters))
	}

	// Sort lexicographically
	sort.Strings(filtered)

	return filtered, nil
}

func decorate(filters []string) []string {
	if len(filters) == 0 {
		filters = append(filters, "*")
	}
	return filters
}
