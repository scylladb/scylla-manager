// Copyright (C) 2017 ScyllaDB

package scyllaclient_test

import (
	"context"
	"net/http"
	"strings"
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/scylladb/scylla-manager/v3/pkg/scyllaclient"
	"github.com/scylladb/scylla-manager/v3/pkg/scyllaclient/scyllaclienttest"
	agentModels "github.com/scylladb/scylla-manager/v3/swagger/gen/agent/models"
)

func TestClientClusterName(t *testing.T) {
	t.Parallel()

	client, closeServer := scyllaclienttest.NewFakeScyllaServer(t, "testdata/scylla_api/storage_service_cluster_name.json")
	defer closeServer()

	v, err := client.ClusterName(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if v != "Test Cluster" {
		t.Fatal(v)
	}
}

func TestClientDatacenters(t *testing.T) {
	t.Parallel()

	m := func(req *http.Request) string {
		if strings.HasPrefix(req.URL.Path, "/storage_service/host_id") {
			return "testdata/scylla_api/host_id_map.json"
		}
		if strings.HasPrefix(req.URL.Path, "/snitch/datacenter") {
			if req.FormValue("host") == "192.168.100.12" {
				return "testdata/scylla_api/host_id_dc_1.json"
			}
			return "testdata/scylla_api/host_id_dc_2.json"
		}
		return ""
	}

	client, closeServer := scyllaclienttest.NewFakeScyllaServerMatching(t, m)
	defer closeServer()

	dcs, err := client.Datacenters(context.Background())
	if err != nil {
		t.Error(err)
	}
	if len(dcs) != 2 {
		t.Errorf("Expected 2 dcs got %d", len(dcs))
	}
}

func TestClientKeyspaces(t *testing.T) {
	t.Parallel()

	client, closeServer := scyllaclienttest.NewFakeScyllaServer(t, "testdata/scylla_api/storage_service_keyspaces.json")
	defer closeServer()

	v, err := client.Keyspaces(context.Background())
	if err != nil {
		t.Fatal(err)
	}

	golden := []string{"test_repair", "system_traces", "system_schema", "system", "test_scylla_manager"}
	if diff := cmp.Diff(v, golden); diff != "" {
		t.Fatal(diff)
	}
}

func TestClientTables(t *testing.T) {
	t.Parallel()

	client, closeServer := scyllaclienttest.NewFakeScyllaServer(t, "testdata/scylla_api/column_family_name.json")
	defer closeServer()

	v, err := client.Tables(context.Background(), "scylla_manager")
	if err != nil {
		t.Fatal(err)
	}

	golden := []string{"event", "repair_run_segment", "repair_config", "scheduler_active_run_by_cluster", "scheduler_task_run", "scheduler_user_task", "repair_run", "repair_unit", "scheduler_task"}
	if diff := cmp.Diff(v, golden); diff != "" {
		t.Fatal(diff)
	}
}

func TestHosts(t *testing.T) {
	t.Parallel()

	client, closeServer := scyllaclienttest.NewFakeScyllaServer(t, "testdata/scylla_api/host_id_map.json")
	defer closeServer()

	v, err := client.Hosts(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(v) != 2 {
		t.Fatal(v)
	}
}

func TestHostIDs(t *testing.T) {
	t.Parallel()

	client, closeServer := scyllaclienttest.NewFakeScyllaServer(t, "testdata/scylla_api/host_id_map.json")
	defer closeServer()

	golden := map[string]string{
		"192.168.100.12": "2938f381-882b-4da7-b94b-e78ad66a5ed4",
		"192.168.100.22": "e0e4aa5a-d908-43a6-ab07-d850c4943150",
	}

	v, err := client.HostIDs(context.Background())
	if err != nil {
		t.Fatal(err)
	}

	if diff := cmp.Diff(v, golden); diff != "" {
		t.Fatal(diff)
	}
}

func TestClientTokens(t *testing.T) {
	t.Parallel()

	client, closeServer := scyllaclienttest.NewFakeScyllaServer(t, "testdata/scylla_api/storage_service_tokens.json")
	defer closeServer()

	v, err := client.Tokens(context.Background(), scyllaclienttest.TestHost)
	if err != nil {
		t.Fatal(err)
	}
	if len(v) != 256 {
		t.Fatal(len(v))
	}
}

func TestClientShardCount(t *testing.T) {
	t.Parallel()

	client, closeServer := scyllaclienttest.NewFakeScyllaServer(t, "testdata/scylla_metrics/metrics")
	defer closeServer()

	for _, host := range []string{scyllaclienttest.TestHost, ""} {
		v, err := client.ShardCount(context.Background(), host)
		if err != nil {
			t.Fatal(err)
		}
		if v != 4 {
			t.Fatal(v)
		}
	}
}

func TestClientTotalMemory(t *testing.T) {
	t.Parallel()

	client, closeServer := scyllaclienttest.NewFakeScyllaServer(t, "testdata/scylla_metrics/metrics")
	defer closeServer()

	v, err := client.TotalMemory(context.Background(), scyllaclienttest.TestHost)
	if err != nil {
		t.Fatal(err)
	}
	if v != 31138512896 {
		t.Fatal(v)
	}
}

func TestClientDescribeRing(t *testing.T) {
	t.Parallel()

	client, closeServer := scyllaclienttest.NewFakeScyllaServer(t, "testdata/scylla_api/describe_ring_scylla_manager.json")
	defer closeServer()

	ring, err := client.DescribeVnodeRing(context.Background(), "scylla_manager")
	if err != nil {
		t.Fatal(err)
	}
	if len(ring.ReplicaTokens[0].Ranges) != 6*256 {
		t.Fatal(len(ring.ReplicaTokens[0].Ranges))
	}
	if len(ring.HostDC) != 6 {
		t.Fatal(len(ring.HostDC))
	}

	{
		golden := scyllaclient.ReplicaTokenRanges{
			ReplicaSet: []string{"172.16.1.10", "172.16.1.2", "172.16.1.20", "172.16.1.3", "172.16.1.4", "172.16.1.5"},
			Ranges:     []scyllaclient.TokenRange{{StartToken: -9223128845313325022, EndToken: -9197905337938558763}},
		}
		if diff := cmp.Diff(ring.ReplicaTokens[0].ReplicaSet, golden.ReplicaSet); diff != "" {
			t.Fatal(diff)
		}
		if diff := cmp.Diff(ring.ReplicaTokens[0].Ranges[0], golden.Ranges[0]); diff != "" {
			t.Fatal(diff)
		}
	}

	{
		golden := map[string]string{
			"172.16.1.10": "dc1",
			"172.16.1.2":  "dc1",
			"172.16.1.20": "dc2",
			"172.16.1.3":  "dc1",
			"172.16.1.4":  "dc2",
			"172.16.1.5":  "dc2",
		}
		if diff := cmp.Diff(ring.HostDC, golden); diff != "" {
			t.Fatal(diff)
		}
	}
}

func TestClientDescribeRingReplicationStrategy(t *testing.T) {
	t.Parallel()

	table := []struct {
		Name     string
		File     string
		Strategy scyllaclient.ReplicationStrategy
	}{
		{
			Name:     "local",
			File:     "testdata/scylla_api/storage_service_describe_ring_system.json",
			Strategy: scyllaclient.LocalStrategy,
		},
		{
			Name:     "simple",
			File:     "testdata/scylla_api/storage_service_describe_ring_system_auth.json",
			Strategy: scyllaclient.SimpleStrategy,
		},
		{
			Name:     "network",
			File:     "testdata/scylla_api/storage_service_describe_ring_test_keyspace_dc2_rf2.json",
			Strategy: scyllaclient.NetworkTopologyStrategy,
		},
	}

	for i := range table {
		test := table[i]

		t.Run(test.Name, func(t *testing.T) {
			t.Parallel()

			client, closeServer := scyllaclienttest.NewFakeScyllaServer(t, test.File)
			defer closeServer()

			ring, err := client.DescribeVnodeRing(context.Background(), "scylla_manager")
			if err != nil {
				t.Fatal(err)
			}
			if ring.Replication != test.Strategy {
				t.Fatal(ring.Replication)
			}
		})
	}
}

func TestClientActiveRepairs(t *testing.T) {
	t.Parallel()

	client, closeServer := scyllaclienttest.NewFakeScyllaServer(t, "testdata/scylla_api/storage_service_active_repair.json")
	defer closeServer()

	v, err := client.ActiveRepairs(context.Background(), []string{scyllaclienttest.TestHost})
	if err != nil {
		t.Fatal(err)
	}
	if diff := cmp.Diff(v, []string{scyllaclienttest.TestHost}); diff != "" {
		t.Fatal(v)
	}
}

func TestClientKillAllRepairs(t *testing.T) {
	t.Parallel()

	client, closeServer := scyllaclienttest.NewFakeScyllaServer(t, "testdata/scylla_api/storage_service_force_terminate_repair.json")
	defer closeServer()

	err := client.KillAllRepairs(context.Background(), scyllaclienttest.TestHost)
	if err != nil {
		t.Fatal(err)
	}
}

func TestClientSnapshotDetails(t *testing.T) {
	t.Parallel()

	client, closeServer := scyllaclienttest.NewFakeScyllaServer(t, "testdata/scylla_api/storage_service_snapshots.json")
	defer closeServer()

	golden := []scyllaclient.Unit{
		{Keyspace: "system_auth", Tables: []string{"role_members", "roles"}},
		{Keyspace: "system_distributed", Tables: []string{"view_build_status"}},
		{Keyspace: "system_traces", Tables: []string{"sessions", "node_slow_log", "events", "node_slow_log_time_idx", "sessions_time_idx"}},
		{Keyspace: "test_keyspace_dc1_rf2", Tables: []string{"void1"}},
		{Keyspace: "test_keyspace_dc1_rf3", Tables: []string{"void1"}},
		{Keyspace: "test_keyspace_dc2_rf2", Tables: []string{"void1"}},
		{Keyspace: "test_keyspace_dc2_rf3", Tables: []string{"void1"}},
		{Keyspace: "test_keyspace_rf2", Tables: []string{"void1"}},
		{Keyspace: "test_keyspace_rf3", Tables: []string{"void1"}},
	}

	v, err := client.SnapshotDetails(context.Background(), scyllaclienttest.TestHost, "sm_4d043260-c352-11e9-a72e-c85b76f42222")
	if err != nil {
		t.Fatal(err)
	}
	if diff := cmp.Diff(v, golden); diff != "" {
		t.Fatal(diff)
	}
}

func TestClientTableDiskSize(t *testing.T) {
	t.Parallel()

	client, closeServer := scyllaclienttest.NewFakeScyllaServer(t, "testdata/scylla_api/column_family_total_disk_space_used.json")
	defer closeServer()

	size, err := client.TableDiskSize(context.Background(), scyllaclienttest.TestHost, "system_schema", "tables")
	if err != nil {
		t.Fatal(err)
	}
	const expected = 4818909025
	if size != expected {
		t.Fatalf("Expected size %d, got %d", expected, size)
	}
}

func TestTakeSnapshotShouldRetryHandler(t *testing.T) {
	t.Run("positive", func(t *testing.T) {
		err := scyllaclienttest.MakeAgentError(agentModels.ErrorResponse{
			Status:  500,
			Message: "std::runtime_error (Keyspace system_auth: snapshot test_snap_1614337496 already exists.)",
		})
		if b := scyllaclient.TakeSnapshotShouldRetryHandler(err); b == nil || *b {
			t.Fatalf("TakeSnapshotShouldRetryHandler() = %v, expected false", b)
		}
	})
	t.Run("negative", func(t *testing.T) {
		err := scyllaclienttest.MakeAgentError(agentModels.ErrorResponse{
			Status: 500,
		})
		if b := scyllaclient.TakeSnapshotShouldRetryHandler(err); b != nil {
			t.Fatalf("TakeSnapshotShouldRetryHandler() = %v, expected nil", b)
		}
	})
}
