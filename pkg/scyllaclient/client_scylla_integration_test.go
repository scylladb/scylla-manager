// Copyright (C) 2017 ScyllaDB

// +build all integration

package scyllaclient_test

import (
	"context"
	"fmt"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	"github.com/scylladb/go-log"
	"github.com/scylladb/go-set/i64set"
	"github.com/scylladb/scylla-manager/pkg/scyllaclient"
	. "github.com/scylladb/scylla-manager/pkg/testutils"
)

func TestClientAuthIntegration(t *testing.T) {
	client, err := scyllaclient.NewClient(scyllaclient.TestConfig(ManagedClusterHosts(), "wrong auth token"), log.NewDevelopment())
	if err != nil {
		t.Fatal(err)
	}
	_, err = client.Hosts(context.Background())
	if err == nil || scyllaclient.StatusCodeOf(err) != http.StatusUnauthorized {
		t.Fatalf("Hosts() error %s expected unauthorized", err)
	}
	if !strings.Contains(err.Error(), "agent [HTTP 401] unauthorized") {
		t.Fatal("expected error about wrong auth token, got", err)
	}
}

func TestClientStatusIntegration(t *testing.T) {
	client, err := scyllaclient.NewClient(scyllaclient.TestConfig(ManagedClusterHosts(), AgentAuthToken()), log.NewDevelopment())
	if err != nil {
		t.Fatal(err)
	}
	status, err := client.Status(context.Background())
	if err != nil {
		t.Fatal("Status() error", err)
	}

	golden := []scyllaclient.NodeStatusInfo{
		{Datacenter: "dc1", Addr: "192.168.100.11", State: "", Status: true},
		{Datacenter: "dc1", Addr: "192.168.100.12", State: "", Status: true},
		{Datacenter: "dc1", Addr: "192.168.100.13", State: "", Status: true},
		{Datacenter: "dc2", Addr: "192.168.100.21", State: "", Status: true},
		{Datacenter: "dc2", Addr: "192.168.100.22", State: "", Status: true},
		{Datacenter: "dc2", Addr: "192.168.100.23", State: "", Status: true},
	}

	if diff := cmp.Diff(status, golden, cmpopts.IgnoreFields(scyllaclient.NodeStatusInfo{}, "HostID")); diff != "" {
		t.Fatalf("Status() = %#+v, diff %s", status, diff)
	}
}

func TestClientActiveRepairsIntegration(t *testing.T) {
	client, err := scyllaclient.NewClient(scyllaclient.TestConfig(ManagedClusterHosts(), AgentAuthToken()), log.NewDevelopment())
	if err != nil {
		t.Fatal(err)
	}
	hosts, err := client.Hosts(context.Background())
	if err != nil {
		t.Fatal(err)
	}

	Print("When: cluster is idle")
	Print("Then: no repairs are running")
	active, err := client.ActiveRepairs(context.Background(), hosts)
	if err != nil {
		t.Fatal(err)
	}
	if len(active) != 0 {
		t.Fatal(active)
	}

	Print("When: repair is running on host 0")
	go func() {
		ExecOnHost(hosts[0], "nodetool repair -pr")
	}()
	defer func() {
		if err := client.KillAllRepairs(context.Background(), hosts[0]); err != nil {
			t.Fatal(err)
		}
	}()

	Print("Then: active repairs reports host 0")
	WaitCond(t, func() bool {
		active, err = client.ActiveRepairs(context.Background(), hosts)
		if err != nil {
			t.Fatal(err)
		}
		return cmp.Diff(active, hosts[0:1]) == ""
	}, 500*time.Millisecond, 4*time.Second)
}

func TestClientSnapshotIntegration(t *testing.T) {
	config := scyllaclient.TestConfig(ManagedClusterHosts(), AgentAuthToken())
	client, err := scyllaclient.NewClient(config, log.NewDevelopment())
	if err != nil {
		t.Fatal(err)
	}

	ctx := context.Background()
	host := ManagedClusterHost()
	tag := fmt.Sprint("test_snap_", time.Now().Unix())

	Print("When: snapshot is taken")
	if err := client.TakeSnapshot(ctx, host, tag, "system_auth"); err != nil {
		t.Fatal(err)
	}

	Print("Then: snapshot is visible on a list")
	snaps, err := client.Snapshots(ctx, host)
	if err != nil {
		t.Fatal(err)
	}
	if !contains(snaps, tag) {
		t.Fatal("missing snapshot", tag)
	}

	Print("When: snapshot is removed")
	if err := client.DeleteSnapshot(ctx, host, tag); err != nil {
		t.Fatal(err)
	}

	Print("Then: there are no snapshots")
	snaps, err = client.Snapshots(ctx, host)
	if err != nil {
		t.Fatal(err)
	}
	if contains(snaps, tag) {
		t.Fatal("snapshot was not deleted", tag)
	}
}

func TestClientTableExistsIntegration(t *testing.T) {
	table := []struct {
		Name     string
		Keyspace string
		Table    string
		Exists   bool
	}{
		{
			Name:     "Valid",
			Keyspace: "system_auth",
			Table:    "roles",
			Exists:   true,
		},
		{
			Name:     "No table",
			Keyspace: "system_auth",
			Table:    "aaa",
			Exists:   false,
		},
		{
			Name:     "No keyspace",
			Keyspace: "aaa",
			Table:    "aaa",
			Exists:   false,
		},
	}

	client, err := scyllaclient.NewClient(scyllaclient.TestConfig(ManagedClusterHosts(), AgentAuthToken()), log.NewDevelopment())
	if err != nil {
		t.Fatal(err)
	}

	ctx := context.Background()

	for i := range table {
		test := table[i]

		exists, err := client.TableExists(ctx, test.Keyspace, test.Table)
		if err != nil {
			t.Fatalf("TableExists failed: %s", err)
		}
		if exists != test.Exists {
			t.Fatalf("TableExists() = %v, expected %v", exists, test.Exists)
		}
	}
}

func TestScyllaFeaturesIntegration(t *testing.T) {
	config := scyllaclient.TestConfig(ManagedClusterHosts(), AgentAuthToken())
	client, err := scyllaclient.NewClient(config, log.NewDevelopment())
	if err != nil {
		t.Fatal(err)
	}

	ctx := context.Background()
	sf, err := client.ScyllaFeatures(ctx, ManagedClusterHosts()...)
	if err != nil {
		t.Error(err)
	}

	for _, h := range ManagedClusterHosts() {
		if !sf[h].RowLevelRepair {
			t.Errorf("%s host doesn't support row-level repair, but it should", h)
		}
		if sf[h].RepairLongPolling {
			t.Errorf("%s host supports long polling repair but it shouldn't", h)
		}
	}
}

func TestClientTokensIntegration(t *testing.T) {
	config := scyllaclient.TestConfig(ManagedClusterHosts(), AgentAuthToken())
	client, err := scyllaclient.NewClient(config, log.NewDevelopment())
	if err != nil {
		t.Fatal(err)
	}

	ctx := context.Background()
	allTokens := i64set.New()
	for _, h := range ManagedClusterHosts() {
		tokens, err := client.Tokens(ctx, h)
		if err != nil {
			t.Error(err)
		}
		if len(tokens) != 256 {
			t.Fatalf("len(Tokens()) = %d, expected 256", len(tokens))
		}
		for _, v := range tokens {
			if allTokens.Has(v) {
				t.Errorf("Token %d already present", v)
			}
			allTokens.Add(v)
		}
	}
}

func contains(v []string, s string) bool {
	for _, e := range v {
		if e == s {
			return true
		}
	}
	return false
}
