package restore

import (
	"slices"
	"strings"

	"github.com/pkg/errors"
)

type dcMappings []dcMapping

type dcMapping struct {
	Source       []string `json:"source"`
	IgnoreSource []string `json:"ignore_source"`
	Target       []string `json:"target"`
	IgnoreTarget []string `json:"ignore_target"`
}

const ignoreDCPrefix = "!"

// Set parses --dc-mapping flag, where the syntax is following:
// ; - used to split different mappings
// => - used to split source => target DCs
// , - used to seprate DCs
// ! - used to ignore DC.
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
		mapping.Source, mapping.IgnoreSource = parseDCList(strings.Split(sourceTargetParts[0], ","))
		mapping.Target, mapping.IgnoreTarget = parseDCList(strings.Split(sourceTargetParts[1], ","))

		*dcm = append(*dcm, mapping)
	}
	return nil
}

func parseDCList(list []string) (dcs, ignore []string) {
	for _, dc := range list {
		if strings.HasPrefix(dc, ignoreDCPrefix) {
			ignore = append(ignore, strings.TrimPrefix(dc, ignoreDCPrefix))
			continue
		}
		dcs = append(dcs, dc)
	}
	return dcs, ignore
}

// String builds --dc-mapping flag back from struct.
func (dcm *dcMappings) String() string {
	if dcm == nil {
		return ""
	}
	var res strings.Builder
	for i, mapping := range *dcm {
		source := slices.Concat(mapping.Source, addIgnorePrefix(mapping.IgnoreSource))
		target := slices.Concat(mapping.Target, addIgnorePrefix(mapping.IgnoreTarget))
		res.WriteString(
			strings.Join(source, ",") + "=>" + strings.Join(target, ","),
		)
		if i != len(*dcm)-1 {
			res.WriteString(";")
		}
	}
	return res.String()
}

func addIgnorePrefix(ignore []string) []string {
	var result []string
	for _, v := range ignore {
		result = append(result, ignoreDCPrefix+v)
	}
	return result
}

// Type implements pflag.Value interface
func (dcm *dcMappings) Type() string {
	return "dc-mapping"
}
