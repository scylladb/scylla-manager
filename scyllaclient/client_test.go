// Copyright (C) 2017 ScyllaDB

package scyllaclient

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/scylladb/go-log"
)

func TestWithPort(t *testing.T) {
	t.Parallel()

	if h := withPort("host", "10000"); h != "host:10000" {
		t.Fatal(h)
	}
	if h := withPort("host:80", "10000"); h != "host:80" {
		t.Fatal(h)
	}
}

func TestClientClusterName(t *testing.T) {
	t.Parallel()

	s := mockServer(t, "testdata/storage_service_cluster_name.json")
	defer s.Close()
	c := testClient(s)

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
			return "testdata/host_id_map.json"
		}
		if strings.HasPrefix(req.URL.Path, "/snitch/datacenter") {
			if req.FormValue("host") == "192.168.100.12" {
				return "testdata/host_id_dc_1.json"
			}
			return "testdata/host_id_dc_2.json"
		}
		return ""
	}

	s := mockServerMatching(t, m)
	defer s.Close()

	c := testClient(s)
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

	s := mockServer(t, "testdata/storage_service_keyspaces.json")
	defer s.Close()
	c := testClient(s)

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

	s := mockServer(t, "testdata/column_family_name.json")
	defer s.Close()
	c := testClient(s)

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

	s := mockServer(t, "testdata/host_id_map.json")
	defer s.Close()
	c := testClient(s)

	v, err := c.Hosts(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(v) != 2 {
		t.Fatal(v)
	}
}

func TestClientTokens(t *testing.T) {
	t.Parallel()

	s := mockServer(t, "testdata/tokens_endpoint.json")
	defer s.Close()
	c := testClient(s)

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

	s := mockServer(t, "testdata/storage_service_partitioner_name.json")
	defer s.Close()
	c := testClient(s)

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

	s := mockServer(t, "testdata/metrics")
	defer s.Close()
	c := testClient(s)

	h := s.Listener.Addr().String()
	v, err := c.ShardCount(context.Background(), h)
	if err != nil {
		t.Fatal(err)
	}
	if v != 4 {
		t.Fatal(v)
	}
}

func TestClientDescribeRing(t *testing.T) {
	t.Parallel()

	s := mockServer(t, "testdata/describe_ring_scylla_manager.json")
	defer s.Close()
	c := testClient(s)

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
			F: "testdata/storage_service_describe_ring_system.json",
			S: LocalStrategy,
		},
		{
			N: "simple",
			F: "testdata/storage_service_describe_ring_system_auth.json",
			S: SimpleStrategy,
		},
		{
			N: "network",
			F: "testdata/storage_service_describe_ring_test_keyspace_dc2_rf2.json",
			S: NetworkTopologyStrategy,
		},
	}

	for _, test := range table {
		t.Run(test.N, func(t *testing.T) {
			t.Parallel()
			s := mockServer(t, test.F)
			defer s.Close()
			c := testClient(s)

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

	s := mockServer(t, "testdata/storage_service_repair_async_scylla_manager_0.json")
	defer s.Close()
	c := testClient(s)

	h := s.Listener.Addr().String()
	v, err := c.Repair(context.Background(), h, &RepairConfig{
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

	s := mockServer(t, "testdata/storage_service_repair_async_scylla_manager_1.json")
	defer s.Close()
	c := testClient(s)

	h := s.Listener.Addr().String()
	v, err := c.RepairStatus(context.Background(), h, "scylla_manager", 1)
	if err != nil {
		t.Fatal(err)
	}
	if v != CommandSuccessful {
		t.Fatal(v)
	}
}

func TestClientRepairStatusForWrongID(t *testing.T) {
	t.Parallel()

	s := mockServer(t, "testdata/storage_service_repair_async_scylla_manager_2.json")
	defer s.Close()
	c := testClient(s)

	h := s.Listener.Addr().String()
	_, err := c.RepairStatus(context.Background(), h, "scylla_manager", 5)
	if err == nil {
		t.Fatal("expected error")
	}
}

func TestClientActiveRepairs(t *testing.T) {
	t.Parallel()

	s := mockServer(t, "testdata/storage_service_active_repair.json")
	defer s.Close()
	c := testClient(s)

	h := s.Listener.Addr().String()
	v, err := c.ActiveRepairs(context.Background(), []string{h})
	if err != nil {
		t.Fatal(err)
	}
	if diff := cmp.Diff(v, []string{h}); diff != "" {
		t.Fatal(v)
	}
}

func TestClientKillAllRepairs(t *testing.T) {
	t.Parallel()

	s := mockServer(t, "testdata/storage_service_force_terminate_repair.json")
	defer s.Close()
	c := testClient(s)

	h := s.Listener.Addr().String()
	err := c.KillAllRepairs(context.Background(), h)
	if err != nil {
		t.Fatal(err)
	}
}

func TestClientPing(t *testing.T) {
	t.Parallel()

	s := mockServer(t, "/dev/null")
	defer s.Close()
	c := testClient(s)

	if _, err := c.Ping(context.Background(), s.Listener.Addr().String()); err != nil {
		t.Fatal(err)
	}

	_, err := c.Ping(context.Background(), "localhost:0")
	if err == nil {
		t.Fatal("expected error")
	}
}

func TestPickNRandomHosts(t *testing.T) {
	table := []struct {
		H []string
		N int
		E int
	}{
		{
			H: []string{"a"},
			N: 1,
			E: 1,
		},
		{
			H: []string{"a"},
			N: 4,
			E: 1,
		},
		{
			H: []string{"a", "a"},
			N: 2,
			E: 2,
		},
		{
			H: []string{"a", "b", "c"},
			N: 2,
			E: 2,
		},
	}

	for i, test := range table {
		picked := pickNRandomHosts(test.N, test.H)
		if len(picked) != test.E {
			t.Errorf("picked %d hosts, expected %d in test %d", len(picked), test.E, i)
		}
	}
}

// Matcher defines a function used to determine the file to return from a given mockServer call.
type Matcher func(req *http.Request) string

// FileMatcher is a simple matcher created for backwards compatibility.
func FileMatcher(file string) Matcher {
	return func(req *http.Request) string {
		return file
	}
}

func mockServer(t *testing.T, file string) *httptest.Server {
	return mockServerMatching(t, FileMatcher(file))
}

func mockServerMatching(t *testing.T, m Matcher) *httptest.Server {
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		r.ParseForm()

		// Emulate ScyllaDB bug
		r.Header.Set("Content-Type", "text/plain")

		file := m(r)

		f, err := os.Open(file)
		if err != nil {
			t.Fatal(err)
		}
		defer f.Close()
		io.Copy(w, f)
	}))
}

func testClient(s *httptest.Server) *Client {
	c, _ := NewClient([]string{s.Listener.Addr().String()}, http.DefaultTransport, log.NewDevelopment())
	return c
}
