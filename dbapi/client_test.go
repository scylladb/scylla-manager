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

func TestClientDatacenterHosts(t *testing.T) {
	t.Parallel()
	s := mockServer(t, "test-fixtures/describe_ring_scylla_management.json")
	defer s.Close()
	c := testClient(s)

	v, err := c.DatacenterHosts(context.Background(), "dc1", "scylla_management")
	if err != nil {
		t.Fatal(err)
	}
	if diff := cmp.Diff(v, []string{"172.16.1.10", "172.16.1.2", "172.16.1.3"}); diff != "" {
		t.Fatal(diff, v)
	}
}

func TestClientTokens(t *testing.T) {
	t.Parallel()
	s := mockServer(t, "test-fixtures/tokens_endpoint.json")
	defer s.Close()
	c := testClient(s)

	v, err := c.Tokens(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(v) != 768 {
		t.Fatal("expected 3 * 256 tokens")
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
	return NewClient(log.NewDevelopmentLogger(), []string{s.Listener.Addr().String()})
}
