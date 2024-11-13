// Copyright (C) 2024 ScyllaDB

package backupspec

import (
	"regexp"
	"strconv"
	"strings"

	"github.com/pkg/errors"
	"github.com/scylladb/go-set/strset"
	"github.com/scylladb/scylla-manager/v3/pkg/util/slice"
	"go.uber.org/multierr"
)

// DCLimit specifies a rate limit for a DC.
type DCLimit struct {
	DC    string `json:"dc"`
	Limit int    `json:"limit"`
}

func (l DCLimit) String() string {
	p := strconv.Itoa(l.Limit)
	if l.DC != "" {
		p = l.DC + ":" + p
	}
	return p
}

// Datacenter returns datacenter that is limited.
func (l DCLimit) Datacenter() string {
	return l.DC
}

func (l DCLimit) MarshalText() (text []byte, err error) {
	return []byte(l.String()), nil
}

func (l *DCLimit) UnmarshalText(text []byte) error {
	pattern := regexp.MustCompile(`^(([a-zA-Z0-9\-\_\.]+):)?([0-9]+)$`)

	m := pattern.FindSubmatch(text)
	if m == nil {
		return errors.Errorf("invalid limit %q, the format is [dc:]<number>", string(text))
	}

	limit, err := strconv.ParseInt(string(m[3]), 10, 64)
	if err != nil {
		return errors.Wrap(err, "invalid limit value")
	}

	l.DC = string(m[2])
	l.Limit = int(limit)

	return nil
}

// Datacenterer describes object that is connected to some datacenter.
type Datacenterer interface {
	Datacenter() string
}

// CheckDCs validates that all dcs are exist in provided dcMap .
func CheckDCs[T Datacenterer](dcs []T, dcMap map[string][]string) (err error) {
	allDCs := strset.New()
	for dc := range dcMap {
		allDCs.Add(dc)
	}

	for _, dc := range dcs {
		if dc.Datacenter() == "" {
			continue
		}
		if !allDCs.Has(dc.Datacenter()) {
			err = multierr.Append(err, errors.Errorf("no such datacenter %s", dc.Datacenter()))
		}
	}
	return
}

// CheckAllDCsCovered validates that all dcToCover exist in provided dcs.
func CheckAllDCsCovered[T Datacenterer](dcs []T, dcToCover []string) error {
	hasDCs := strset.New()
	hasDefault := false

	for _, dc := range dcs {
		if dc.Datacenter() == "" {
			hasDefault = true
			continue
		}
		hasDCs.Add(dc.Datacenter())
	}

	if !hasDefault {
		if d := strset.Difference(strset.New(dcToCover...), hasDCs); !d.IsEmpty() {
			msg := "missing object(s) for datacenters %s"
			if d.Size() == 1 {
				msg = "missing object for datacenter %s"
			}
			return errors.Errorf(msg, strings.Join(d.List(), ", "))
		}
	}

	return nil
}

// FilterDCs returns dcs present in filteredDCs.
func FilterDCs[T Datacenterer](dcs []T, filteredDCs []string) []T {
	var filtered []T
	for _, dc := range dcs {
		if dc.Datacenter() == "" || slice.ContainsString(filteredDCs, dc.Datacenter()) {
			filtered = append(filtered, dc)
			continue
		}
	}
	return filtered
}
