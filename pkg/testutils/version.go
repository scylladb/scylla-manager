// Copyright (C) 2024 ScyllaDB

package testutils

import (
	"testing"

	"github.com/scylladb/scylla-manager/v3/pkg/scyllaclient"
	"github.com/scylladb/scylla-manager/v3/pkg/util/version"
)

// CheckAnyConstraint checks if any of the passed version constraints are satisfied.
// Can be used for skipping tests which make sense only for certain Scylla versions.
func CheckAnyConstraint(t *testing.T, client *scyllaclient.Client, constraints ...string) bool {
	t.Helper()

	ni, err := client.AnyNodeInfo(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	for _, c := range constraints {
		ok, err := version.CheckConstraint(ni.ScyllaVersion, c)
		if err != nil {
			t.Fatal(err)
		}
		if ok {
			return true
		}
	}
	return false
}

// CheckConstraint calls version.CheckConstraint and fails in case of an error.
func CheckConstraint(t *testing.T, v, c string) bool {
	t.Helper()

	ok, err := version.CheckConstraint(v, c)
	if err != nil {
		t.Fatal(err)
	}
	return ok
}
