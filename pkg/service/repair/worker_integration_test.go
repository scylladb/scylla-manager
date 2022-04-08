// Copyright (C) 2017 ScyllaDB

//go:build all || integration
// +build all integration

package repair

import (
	"context"
	"testing"

	"github.com/scylladb/go-log"
	"github.com/scylladb/scylla-manager/v3/pkg/scyllaclient"
	. "github.com/scylladb/scylla-manager/v3/pkg/testutils"
)

func TestTableDeletedIntegration(t *testing.T) {
	client, err := scyllaclient.NewClient(scyllaclient.TestConfig(ManagedClusterHosts(), AgentAuthToken()), log.NewDevelopment())
	if err != nil {
		t.Fatal(err)
	}

	w := worker{
		client: client,
	}

	ctx := context.Background()

	t.Run("No table", func(t *testing.T) {
		id, err := client.Repair(ctx, ManagedClusterHost(), scyllaclient.RepairConfig{
			Keyspace: "system_auth",
			Tables:   []string{"aaa"},
			Ranges:   "10:20",
		})
		if err != nil {
			t.Fatalf("Repair() error %s", err)
		}

		_, err = client.RepairStatus(ctx, ManagedClusterHost(), "system_auth", id, 0)
		if !w.tableDeleted(ctx, err, "system_auth", "aaa") {
			t.Errorf("tableDeleted() = %v, expected %v", false, true)
		}
	})

	t.Run("No keyspace", func(t *testing.T) {
		_, err := client.Repair(ctx, ManagedClusterHost(), scyllaclient.RepairConfig{
			Keyspace: "aaa",
			Tables:   []string{"aaa"},
			Ranges:   "10:20",
		})
		if !w.tableDeleted(ctx, err, "aaa", "aaa") {
			t.Errorf("tableDeleted() = %v, expected %v", false, true)
		}
	})
}
