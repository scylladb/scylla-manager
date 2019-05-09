// Copyright (C) 2017 ScyllaDB

// +build all integration

package scyllaclient_test

import (
	"context"
	"fmt"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/scylladb/go-log"
	. "github.com/scylladb/mermaid/mermaidtest"
	"github.com/scylladb/mermaid/scyllaclient"
)

func TestClientDialOnceAndCloseIntegration(t *testing.T) {
	// Protects conns.
	var mu sync.Mutex
	var conns []net.Conn

	s := NewSSHTransport()
	d := s.DialContext
	s.DialContext = func(ctx context.Context, network, addr string) (conn net.Conn, err error) {
		conn, err = d(ctx, network, addr)
		mu.Lock()
		conns = append(conns, conn)
		mu.Unlock()
		return
	}

	config := scyllaclient.DefaultConfig()
	config.Hosts = ManagedClusterHosts
	config.Transport = s

	client, err := scyllaclient.NewClient(config, log.NewDevelopment())
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 10; i++ {
		_, err := client.Keyspaces(context.Background())
		if err != nil {
			t.Fatal(err)
		}
	}
	mu.Lock()
	defer mu.Unlock()
	if len(conns) != 1 {
		t.Fatal("expected dial once, got", len(conns))
	}

	if err := client.Close(); err != nil {
		t.Fatal(err)
	}
	if err := conns[0].SetDeadline(time.Time{}); err == nil {
		t.Fatal("expected connection to be closed")
	}
}

func TestClientClosestDCIntegration(t *testing.T) {
	config := scyllaclient.DefaultConfig()
	config.Hosts = ManagedClusterHosts
	config.Transport = NewSSHTransport()

	client, err := scyllaclient.NewClient(config, log.NewDevelopment())
	if err != nil {
		t.Fatal(err)
	}

	dcs := map[string][]string{
		"dc1": ManagedClusterHosts,
		"xx":  {"xx.xx.xx.xx"},
	}

	closest, err := client.ClosestDC(context.Background(), dcs)
	if err != nil {
		t.Fatal(err)
	}
	if diff := cmp.Diff(closest, []string{"dc1", "xx"}); diff != "" {
		t.Fatal(closest, diff)
	}
}

func TestClientActiveRepairsIntegration(t *testing.T) {
	config := scyllaclient.DefaultConfig()
	config.Hosts = ManagedClusterHosts
	config.Transport = NewSSHTransport()

	client, err := scyllaclient.NewClient(config, log.NewDevelopment())
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
		_, _, err := ExecOnHost(context.Background(), hosts[0], "nodetool repair -pr")
		if err != nil {
			t.Log(err)
		}
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
	config := scyllaclient.DefaultConfig()
	config.Hosts = ManagedClusterHosts
	config.Transport = NewSSHTransport()

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
