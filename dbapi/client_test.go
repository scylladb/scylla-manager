// Copyright (C) 2017 ScyllaDB

package dbapi

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/scylladb/mermaid/log"
)

func TestWithPort(t *testing.T) {
	t.Parallel()

	if h := withPort("host"); h != "host:10000" {
		t.Fatal(h)
	}
	if h := withPort("host:80"); h != "host:80" {
		t.Fatal(h)
	}
}

func TestClientDatacenter(t *testing.T) {
	t.Parallel()

	s := mockServer(t, "testdata/snitch_datacenter.json")
	defer s.Close()
	c := testClient(s)

	v, err := c.Datacenter(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if v != "dc1" {
		t.Fatal(v)
	}
}

func TestClientDescribeRing(t *testing.T) {
	t.Parallel()

	s := mockServer(t, "testdata/describe_ring_scylla_management.json")
	defer s.Close()
	c := testClient(s)

	dcs, trs, err := c.DescribeRing(context.Background(), "scylla_management")
	if err != nil {
		t.Fatal(err)
	}
	if diff := cmp.Diff(dcs, []string{"dc1", "dc2"}); diff != "" {
		t.Fatal(diff)
	}
	if len(trs) != 6*256 {
		t.Fatal(len(trs))
	}

	expected := &TokenRange{
		StartToken: 9170930477372008214,
		EndToken:   9192981293347332843,
		Hosts:      map[string][]string{"dc1": {"172.16.1.10", "172.16.1.2", "172.16.1.3"}, "dc2": {"172.16.1.4", "172.16.1.20", "172.16.1.5"}},
	}
	if diff := cmp.Diff(trs[0], expected); diff != "" {
		t.Fatal(diff)
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

func TestClientTables(t *testing.T) {
	t.Parallel()

	s := mockServer(t, "testdata/column_family_name.json")
	defer s.Close()
	c := testClient(s)

	v, err := c.Tables(context.Background(), "scylla_management")
	if err != nil {
		t.Fatal(err)
	}
	expected := []string{"event", "repair_run_segment", "repair_config", "scheduler_active_run_by_cluster", "scheduler_task_run", "scheduler_user_task", "repair_run", "repair_unit", "scheduler_task"}
	if diff := cmp.Diff(v, expected); diff != "" {
		t.Fatal(diff)
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

func mockServer(t *testing.T, file string) *httptest.Server {
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// emulate ScyllaDB bug
		r.Header.Set("Content-Type", "text/plain")

		f, err := os.Open(file)
		if err != nil {
			t.Fatal(err)
		}
		defer f.Close()
		io.Copy(w, f)
	}))
}

func testClient(s *httptest.Server) *Client {
	c, _ := NewClient([]string{s.Listener.Addr().String()}, log.NewDevelopmentLogger())
	return c
}
