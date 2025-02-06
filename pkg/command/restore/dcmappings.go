// Copyright (C) 2025 ScyllaDB

package restore

import (
	"strings"

	"github.com/pkg/errors"
)

type dcMappings []dcMapping

type dcMapping struct {
	Source []string `json:"source"`
	Target []string `json:"target"`
}

// Set parses --dc-mapping flag, where the syntax is following:
// ; - used to split different mappings
// => - used to split source => target DCs
// , - used to seprate DCs.
func (dcm *dcMappings) Set(v string) error {
	mappingParts := strings.Split(v, ";")
	for _, dcMapPart := range mappingParts {
		sourceTargetParts := strings.Split(dcMapPart, "=>")
		if len(sourceTargetParts) != 2 {
			return errors.New("invalid syntax, mapping should be in a format of sourceDcs=>targetDcs, but got: " + dcMapPart)
		}
		if sourceTargetParts[0] == "" || sourceTargetParts[1] == "" {
			return errors.New("invalid syntax, mapping should be in a format of sourceDcs=>targetDcs, but got: " + dcMapPart)
		}

		var mapping dcMapping
		mapping.Source = parseDCList(sourceTargetParts[0])
		mapping.Target = parseDCList(sourceTargetParts[1])

		*dcm = append(*dcm, mapping)
	}
	return nil
}

func parseDCList(raw string) []string {
	dcs := strings.Split(raw, ",")
	for i, dc := range dcs {
		dcs[i] = strings.TrimSpace(dc)
	}
	return dcs
}

// String builds --dc-mapping flag back from struct.
func (dcm *dcMappings) String() string {
	if dcm == nil {
		return ""
	}
	var res strings.Builder
	for i, mapping := range *dcm {
		res.WriteString(
			strings.Join(mapping.Source, ",") + "=>" + strings.Join(mapping.Target, ","),
		)
		if i != len(*dcm)-1 {
			res.WriteString(";")
		}
	}
	return res.String()
}

// Type implements pflag.Value interface.
func (dcm *dcMappings) Type() string {
	return "dc-mapping"
}
