// Copyright (C) 2017 ScyllaDB

package version

import (
	"fmt"
	"strings"

	"github.com/hashicorp/go-version"
)

// Short excludes any metadata or pre-release information. For example,
// for a version "1.2.3-20200101.b41b3dbs1b", it will return "1.2.3".
// If provided string isn't version, it will return input string.
func Short(v string) string {
	ver, err := version.NewVersion(v)
	if err != nil {
		return v
	}

	parts := make([]string, len(ver.Segments()))
	for i, s := range ver.Segments() {
		parts[i] = fmt.Sprint(s)
	}

	return strings.Join(parts, ".")
}
