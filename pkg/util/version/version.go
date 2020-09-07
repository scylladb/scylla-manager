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

const (
	masterMajorVersion           = "666"
	masterEnterpriseMajorVersion = "9999"

	masterVersionSuffix           = ".dev"
	masterVersionLongSuffix       = ".development"
	masterEnterpriseVersionSuffix = ".enterprise_dev"

	masterVersion                 = masterMajorVersion + masterVersionSuffix
	masterLongVersion             = masterMajorVersion + masterVersionLongSuffix
	masterEnterpriseMasterVersion = masterEnterpriseMajorVersion + masterEnterpriseVersionSuffix
)

// MasterVersion returns whether provided version string originates from master branch.
func MasterVersion(v string) bool {
	v = strings.Split(v, "-")[0]
	return v == masterLongVersion || v == masterEnterpriseMasterVersion || v == masterVersion
}

// TrimMaster returns version string without master branch bloat breaking semantic
// format constraints.
func TrimMaster(v string) string {
	if v == "Snapshot" {
		return masterMajorVersion
	}

	v = strings.Split(v, "-")[0]
	v = strings.TrimSuffix(v, masterVersionSuffix)
	v = strings.TrimSuffix(v, masterVersionLongSuffix)
	v = strings.TrimSuffix(v, masterEnterpriseVersionSuffix)

	return v
}
