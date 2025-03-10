// Copyright (C) 2024 ScyllaDB

package testutils

import (
	"context"
	"testing"

	"github.com/scylladb/scylla-manager/v3/pkg/scyllaclient"
	"github.com/scylladb/scylla-manager/v3/pkg/util/version"
)

// CheckAnyConstraint checks if any of the passed version constraints are satisfied.
// Can be used for skipping tests which make sense only for certain Scylla versions.
func CheckAnyConstraint(t *testing.T, client *scyllaclient.Client, constraints ...string) bool {
	t.Helper()

	ni, err := client.AnyNodeInfo(context.Background())
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
