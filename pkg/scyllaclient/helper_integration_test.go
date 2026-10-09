// Copyright (C) 2026 ScyllaDB

//go:build all || integration

package scyllaclient_test

import (
	"testing"

	"github.com/scylladb/go-log"
	"github.com/scylladb/scylla-manager/v3/pkg/scyllaclient"
)

// newTestClient creates scylla client which is closed on t.Cleanup.
func newTestClient(t *testing.T, config scyllaclient.Config, logger log.Logger) *scyllaclient.Client {
	t.Helper()

	client, err := scyllaclient.NewClient(config, logger)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := client.Close(); err != nil {
			t.Errorf("close scylla client: %v", err)
		}
	})
	return client
}
