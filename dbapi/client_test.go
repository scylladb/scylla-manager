package dbapi

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"

	"github.com/scylladb/mermaid/log"
)

func TestClientTokens(t *testing.T) {
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/storage_service/tokens_endpoint" {
			t.Fatal("wront URL path", r.URL.Path)
		}

		f, err := os.Open("test-fixtures/tokens_endpoint_0.json")
		if err != nil {
			t.Fatal(err)
		}
		defer f.Close()
		io.Copy(w, f)
	}))
	defer s.Close()

	c := NewClient(log.NewDevelopmentLogger())

	v, err := c.Tokens(context.Background(), s.Listener.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	if len(v) != 768 {
		t.Fatal("expected 3 * 256 tokens")
	}
}
