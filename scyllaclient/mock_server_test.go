// Copyright (C) 2017 ScyllaDB

package scyllaclient

import (
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"

	"github.com/scylladb/go-log"
)

// Matcher defines a function used to determine the file to return from a given newMockServer call.
type Matcher func(req *http.Request) string

// FileMatcher is a simple matcher created for backwards compatibility.
func FileMatcher(file string) Matcher {
	return func(req *http.Request) string {
		return file
	}
}

func newMockServer(t *testing.T, file string) (*Client, func()) {
	return newMockServerMatching(t, FileMatcher(file))
}

func newMockServerMatching(t *testing.T, m Matcher) (*Client, func()) {
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
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

	config := DefaultConfig()
	host, port, _ := net.SplitHostPort(s.Listener.Addr().String())
	config.Hosts = []string{host}
	config.Scheme = "http"
	config.AgentPort = port

	c, err := NewClient(config, log.NewDevelopment())
	if err != nil {
		t.Fatal(err)
	}

	return c, s.Close
}

const testHost = "127.0.0.1"
