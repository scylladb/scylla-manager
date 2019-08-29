// Copyright (C) 2017 ScyllaDB

// +build all integration

package scyllaclient_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/scylladb/go-log"
	. "github.com/scylladb/mermaid/mermaidtest"
	"github.com/scylladb/mermaid/scyllaclient"
)

func TestClientActiveRepairsIntegration(t *testing.T) {
	client, err := scyllaclient.NewClient(scyllaclient.TestConfig(ManagedClusterHosts, AgentAuthToken()), log.NewDevelopment())
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
	config := scyllaclient.TestConfig(ManagedClusterHosts, AgentAuthToken())
	client, err := scyllaclient.NewClient(config, log.NewDevelopment())
	if err != nil {
		t.Fatal(err)
	}

	ctx := context.Background()
	host := ManagedClusterHosts[0]
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

func contains(v []string, s string) bool {
	for _, e := range v {
		if e == s {
			return true
		}
	}
	return false
}
