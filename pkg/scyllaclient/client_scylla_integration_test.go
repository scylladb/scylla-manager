// Copyright (C) 2017 ScyllaDB

//go:build all || integration
// +build all integration

package scyllaclient_test

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/scylladb/scylla-manager/v3/pkg/testutils/db"
	. "github.com/scylladb/scylla-manager/v3/pkg/testutils/testconfig"
	"github.com/scylladb/scylla-manager/v3/pkg/util/maputil"

	"github.com/google/go-cmp/cmp"
	"github.com/scylladb/go-log"
	"github.com/scylladb/go-set/i64set"
	"github.com/scylladb/scylla-manager/v3/pkg/scyllaclient"
	. "github.com/scylladb/scylla-manager/v3/pkg/testutils"
	"github.com/scylladb/scylla-manager/v3/pkg/util/slice"
	"github.com/scylladb/scylla-manager/v3/pkg/util/timeutc"
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

	ipCmp := cmp.Comparer(func(n1, n2 scyllaclient.NodeStatusInfo) bool {
		out := true
		out = out && n1.Datacenter == n2.Datacenter
		out = out && n1.State == n2.State
		out = out && n1.Status == n2.Status
		out = out && net.ParseIP(n1.Addr).Equal(net.ParseIP(n2.Addr))
		return out
	})

	golden := scyllaclient.NodeStatusInfoSlice{
		{Datacenter: "dc1", Addr: IPFromTestNet("11"), State: "", Status: true},
		{Datacenter: "dc1", Addr: IPFromTestNet("12"), State: "", Status: true},
		{Datacenter: "dc1", Addr: IPFromTestNet("13"), State: "", Status: true},
		{Datacenter: "dc2", Addr: IPFromTestNet("21"), State: "", Status: true},
		{Datacenter: "dc2", Addr: IPFromTestNet("22"), State: "", Status: true},
		{Datacenter: "dc2", Addr: IPFromTestNet("23"), State: "", Status: true},
	}

	if diff := cmp.Diff(golden, status, ipCmp); diff != "" {
		t.Fatalf("Status() = %#+v, diff %s", status, diff)
	}
}

func TestClientDescribeRingIntegration(t *testing.T) {
	testCases := []struct {
		name            string
		replicationStmt string
		replication     scyllaclient.ReplicationStrategy
		rf              int
		dcRF            map[string]int
	}{
		{
			name:            "simple_4",
			replicationStmt: "{'class': 'SimpleStrategy', 'replication_factor': 4}",
			replication:     scyllaclient.SimpleStrategy,
			rf:              4,
		},
		{
			name:            "network_topology_1",
			replicationStmt: "{'class': 'NetworkTopologyStrategy', 'dc1': 1}",
			replication:     scyllaclient.NetworkTopologyStrategy,
			rf:              1,
			dcRF: map[string]int{
				"dc1": 1,
			},
		},
		{
			name:            "network_topology_2_2",
			replicationStmt: "{'class': 'NetworkTopologyStrategy', 'dc1': 2, 'dc2': 2}",
			replication:     scyllaclient.NetworkTopologyStrategy,
			rf:              4,
			dcRF: map[string]int{
				"dc1": 2,
				"dc2": 2,
			},
		},
		{
			name:            "network_topology_3_3",
			replicationStmt: "{'class': 'NetworkTopologyStrategy', 'dc1': 3, 'dc2': 3}",
			replication:     scyllaclient.NetworkTopologyStrategy,
			rf:              6,
			dcRF: map[string]int{
				"dc1": 3,
				"dc2": 3,
			},
		},
	}

	client, err := scyllaclient.NewClient(scyllaclient.TestConfig(ManagedClusterHosts(), AgentAuthToken()), log.NewDevelopment())
	if err != nil {
		t.Fatal(err)
	}
	clusterSession := db.CreateSessionAndDropAllKeyspaces(t, client)
	defer clusterSession.Close()

	ringDescriber := scyllaclient.NewRingDescriber(context.Background(), client)
	for i := range testCases {
		tc := testCases[i]
		t.Run(tc.name, func(t *testing.T) {
			if err := clusterSession.ExecStmt(fmt.Sprintf("CREATE KEYSPACE %s WITH replication = %s", tc.name, tc.replicationStmt)); err != nil {
				t.Fatal(err)
			}
			defer func() {
				if err := clusterSession.ExecStmt("DROP KEYSPACE " + tc.name); err != nil {
					t.Fatal(err)
				}
			}()
			if err := clusterSession.ExecStmt(fmt.Sprintf("CREATE TABLE %s.test_tab (a int PRIMARY KEY, b int)", tc.name)); err != nil {
				t.Fatal(err)
			}

			ringDescriber.Reset(context.Background())
			if ringDescriber.IsTabletKeyspace(tc.name) {
				// This log is for checking whether testing setup correctly enabled tablets
				t.Log("Tablet keyspace exists!")
			}
			ring, err := ringDescriber.DescribeRing(context.Background(), tc.name, "test_tab")
			if err != nil {
				t.Fatal(err)
			}
			if tc.replication != ring.Replication {
				t.Fatalf("Replication: expected %s, got %s", tc.replication, ring.Replication)
			}
			if tc.rf != ring.RF {
				t.Fatalf("RF: expected %d, got %d", tc.rf, ring.RF)
			}
			if !maputil.Equal(tc.dcRF, ring.DCrf) {
				t.Fatalf("DCrf: expected %v, got %v", tc.dcRF, ring.DCrf)
			}
		})
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
	tag := "sm_" + timeutc.Now().Format("20060102150405") + "UTC"
	const ks = "system_schema"
	const tab = "tables"

	Print("When: snapshot is taken")
	if err := client.TakeSnapshot(ctx, host, tag, ks); err != nil {
		t.Fatal(err)
	}

	Print("Then: snapshot is visible on a list")
	snaps, err := client.Snapshots(ctx, host)
	if err != nil {
		t.Fatal(err)
	}
	if !slice.ContainsString(snaps, tag) {
		t.Fatal("missing snapshot", tag)
	}

	Print("When: snapshot is taken again with the same tag")
	Print("Then: nothing happens")
	if err := client.TakeSnapshot(ctx, host, tag, ks); err != nil {
		t.Fatal(err)
	}

	Print("When: table snapshot is removed")
	if err := client.DeleteTableSnapshot(ctx, host, tag, ks, tab); err != nil {
		t.Fatal(err)
	}

	Print("Then: table is missing from snapshot")
	units, err := client.SnapshotDetails(ctx, host, tag)
	if err != nil {
		t.Fatal(err)
	}
	var u scyllaclient.Unit
	for _, u = range units {
		if u.Keyspace == ks {
			break
		}
	}
	if u.Keyspace == "" {
		t.Fatal("missing snapshot")
	}
	if slice.ContainsString(u.Tables, tab) {
		t.Fatal("table snapshot not deleted")
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
	if slice.ContainsString(snaps, tag) {
		t.Fatal("snapshot was not deleted", tag)
	}
}

func TestClientTableExistsIntegration(t *testing.T) {
	const ks = "system_schema"
	const tab = "tables"
	table := []struct {
		Name     string
		Keyspace string
		Table    string
		Exists   bool
	}{
		{
			Name:     "Valid",
			Keyspace: ks,
			Table:    tab,
			Exists:   true,
		},
		{
			Name:     "No table",
			Keyspace: ks,
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

		exists, err := client.TableExists(ctx, "", test.Keyspace, test.Table)
		if err != nil {
			t.Fatalf("TableExists failed: %s", err)
		}
		if exists != test.Exists {
			t.Fatalf("TableExists() = %v, expected %v", exists, test.Exists)
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
