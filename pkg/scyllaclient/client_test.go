// Copyright (C) 2026 ScyllaDB

package scyllaclient_test

import (
	"testing"

	"github.com/scylladb/scylla-manager/v3/pkg/scyllaclient/scyllaclienttest"
)

func TestClientClose(t *testing.T) {
	t.Parallel()

	client, closeServer := scyllaclienttest.NewFakeScyllaServer(t, "testdata/scylla_api/host_id_map_localhost.json")
	defer closeServer()

	// Closing multiple times does not error nor panic
	for i := range 2 {
		if err := client.Close(); err != nil {
			t.Fatalf("Close() %d error: %s", i, err)
		}
	}

	// Closed client remains usable
	if _, err := client.HostIDs(t.Context()); err != nil {
		t.Fatalf("HostIDs() after Close() error: %s", err)
	}
}
