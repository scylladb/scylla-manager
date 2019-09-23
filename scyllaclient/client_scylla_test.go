// Copyright (C) 2017 ScyllaDB

package scyllaclient

import (
	"context"
	"net/http"
	"strings"
	"testing"

	"github.com/google/go-cmp/cmp"
)

func TestClientClusterName(t *testing.T) {
	t.Parallel()

	c, close := newMockServer(t, "testdata/scylla_api/storage_service_cluster_name.json")
	defer close()

	v, err := c.ClusterName(context.Background())
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

	c, close := newMockServerMatching(t, m)
	defer close()

	dcs, err := c.Datacenters(context.Background())
	if err != nil {
		t.Error(err)
	}
	if len(dcs) != 2 {
		t.Errorf("expected 2 dcs got %d", len(dcs))
	}
}

func TestClientKeyspaces(t *testing.T) {
	t.Parallel()

	c, close := newMockServer(t, "testdata/scylla_api/storage_service_keyspaces.json")
	defer close()

	v, err := c.Keyspaces(context.Background())
	if err != nil {
		t.Fatal(err)
	}

	expected := []string{"test_repair", "system_traces", "system_schema", "system", "test_scylla_manager"}
	if diff := cmp.Diff(v, expected); diff != "" {
		t.Fatal(diff)
	}
}

func TestClientTables(t *testing.T) {
	t.Parallel()

	c, close := newMockServer(t, "testdata/scylla_api/column_family_name.json")
	defer close()

	v, err := c.Tables(context.Background(), "scylla_manager")
	if err != nil {
		t.Fatal(err)
	}
	expected := []string{"event", "repair_run_segment", "repair_config", "scheduler_active_run_by_cluster", "scheduler_task_run", "scheduler_user_task", "repair_run", "repair_unit", "scheduler_task"}
	if diff := cmp.Diff(v, expected); diff != "" {
		t.Fatal(diff)
	}
}

func TestHosts(t *testing.T) {
	t.Parallel()

	c, close := newMockServer(t, "testdata/scylla_api/host_id_map.json")
	defer close()

	v, err := c.Hosts(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(v) != 2 {
		t.Fatal(v)
	}
}

func TestHostIDs(t *testing.T) {
	t.Parallel()

	c, close := newMockServer(t, "testdata/scylla_api/host_id_map.json")
	defer close()

	golden := map[string]string{
		"192.168.100.12": "2938f381-882b-4da7-b94b-e78ad66a5ed4",
		"192.168.100.22": "e0e4aa5a-d908-43a6-ab07-d850c4943150",
	}

	v, err := c.HostIDs(context.Background())
	if err != nil {
		t.Fatal(err)
	}

	if diff := cmp.Diff(v, golden); diff != "" {
		t.Fatal(diff)
	}
}

func TestClientTokens(t *testing.T) {
	t.Parallel()

	c, close := newMockServer(t, "testdata/scylla_api/tokens_endpoint.json")
	defer close()

	v, err := c.Tokens(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(v) != 3*256 {
		t.Fatal(len(v))
	}
}

func TestClientPartitioner(t *testing.T) {
	t.Parallel()

	c, close := newMockServer(t, "testdata/scylla_api/storage_service_partitioner_name.json")
	defer close()

	v, err := c.Partitioner(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if v != Murmur3Partitioner {
		t.Fatal(v)
	}
}

func TestClientShardCount(t *testing.T) {
	t.Parallel()

	c, close := newMockServer(t, "testdata/scylla_metrics/metrics")
	defer close()

	v, err := c.ShardCount(context.Background(), testHost)
	if err != nil {
		t.Fatal(err)
	}
	if v != 4 {
		t.Fatal(v)
	}
}

func TestClientDescribeRing(t *testing.T) {
	t.Parallel()

	c, close := newMockServer(t, "testdata/scylla_api/describe_ring_scylla_manager.json")
	defer close()

	ring, err := c.DescribeRing(context.Background(), "scylla_manager")
	if err != nil {
		t.Fatal(err)
	}
	if len(ring.Tokens) != 6*256 {
		t.Fatal(len(ring.Tokens))
	}
	if len(ring.HostDC) != 6 {
		t.Fatal(len(ring.HostDC))
	}

	{
		expected := TokenRange{
			StartToken: 9170930477372008214,
			EndToken:   9192981293347332843,
			Replicas:   []string{"172.16.1.10", "172.16.1.4", "172.16.1.2", "172.16.1.3", "172.16.1.20", "172.16.1.5"},
		}
		if diff := cmp.Diff(ring.Tokens[0], expected); diff != "" {
			t.Fatal(diff)
		}
	}

	{
		expected := map[string]string{
			"172.16.1.10": "dc1",
			"172.16.1.2":  "dc1",
			"172.16.1.20": "dc2",
			"172.16.1.3":  "dc1",
			"172.16.1.4":  "dc2",
			"172.16.1.5":  "dc2",
		}
		if diff := cmp.Diff(ring.HostDC, expected); diff != "" {
			t.Fatal(diff)
		}
	}
}

func TestClientDescribeRingReplicationStrategy(t *testing.T) {
	t.Parallel()

	table := []struct {
		N string
		F string
		S ReplicationStrategy
	}{
		{
			N: "local",
			F: "testdata/scylla_api/storage_service_describe_ring_system.json",
			S: LocalStrategy,
		},
		{
			N: "simple",
			F: "testdata/scylla_api/storage_service_describe_ring_system_auth.json",
			S: SimpleStrategy,
		},
		{
			N: "network",
			F: "testdata/scylla_api/storage_service_describe_ring_test_keyspace_dc2_rf2.json",
			S: NetworkTopologyStrategy,
		},
	}

	for _, test := range table {
		t.Run(test.N, func(t *testing.T) {
			t.Parallel()
			c, close := newMockServer(t, test.F)
			defer close()

			ring, err := c.DescribeRing(context.Background(), "scylla_manager")
			if err != nil {
				t.Fatal(err)
			}
			if ring.Replication != test.S {
				t.Fatal(ring.Replication)
			}
		})
	}
}

func TestClientRepair(t *testing.T) {
	t.Parallel()

	c, close := newMockServer(t, "testdata/scylla_api/storage_service_repair_async_scylla_manager_0.json")
	defer close()

	v, err := c.Repair(context.Background(), testHost, &RepairConfig{
		Keyspace: "scylla_manager",
		Ranges:   "100:110,120:130",
	})
	if err != nil {
		t.Fatal(err)
	}
	if v != 1 {
		t.Fatal(v)
	}
}

func TestClientRepairStatus(t *testing.T) {
	t.Parallel()

	c, close := newMockServer(t, "testdata/scylla_api/storage_service_repair_async_scylla_manager_1.json")
	defer close()

	v, err := c.RepairStatus(context.Background(), testHost, "scylla_manager", 1)
	if err != nil {
		t.Fatal(err)
	}
	if v != CommandSuccessful {
		t.Fatal(v)
	}
}

func TestClientRepairStatusForWrongID(t *testing.T) {
	t.Parallel()

	c, close := newMockServer(t, "testdata/scylla_api/storage_service_repair_async_scylla_manager_2.json")
	defer close()

	_, err := c.RepairStatus(context.Background(), testHost, "scylla_manager", 5)
	if err == nil {
		t.Fatal("expected error")
	}
}

func TestClientActiveRepairs(t *testing.T) {
	t.Parallel()

	c, close := newMockServer(t, "testdata/scylla_api/storage_service_active_repair.json")
	defer close()

	v, err := c.ActiveRepairs(context.Background(), []string{testHost})
	if err != nil {
		t.Fatal(err)
	}
	if diff := cmp.Diff(v, []string{testHost}); diff != "" {
		t.Fatal(v)
	}
}

func TestClientKillAllRepairs(t *testing.T) {
	t.Parallel()

	c, close := newMockServer(t, "testdata/scylla_api/storage_service_force_terminate_repair.json")
	defer close()

	err := c.KillAllRepairs(context.Background(), testHost)
	if err != nil {
		t.Fatal(err)
	}
}

func TestClientSnapshotDetails(t *testing.T) {
	t.Parallel()
	c, close := newMockServer(t, "testdata/scylla_api/storage_service_snapshots.json")
	defer close()

	golden := []Unit{
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
	v, err := c.SnapshotDetails(context.Background(), testHost, "sm_4d043260-c352-11e9-a72e-c85b76f42222")
	if err != nil {
		t.Fatal(err)
	}
	if diff := cmp.Diff(v, golden); diff != "" {
		t.Fatal(diff)
	}
}

func TestClientTableDiskSize(t *testing.T) {
	t.Parallel()
	c, cl := newMockServer(t, "testdata/scylla_api/column_family_total_disk_space_used.json")
	defer cl()

	size, err := c.TableDiskSize(context.Background(), testHost, "system_schema", "tables")
	if err != nil {
		t.Fatal(err)
	}
	const expected = 149140
	if size != expected {
		t.Fatalf("Expected size %d, got %d", expected, size)
	}
}
