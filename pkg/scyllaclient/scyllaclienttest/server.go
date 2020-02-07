// Copyright (C) 2017 ScyllaDB

package scyllaclienttest

import (
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"

	"go.uber.org/atomic"
)

// ServerOption allows to modify test Server before it's started.
type ServerOption func(*httptest.Server)

// ServerListenOnAddr is ServerOption that allows to specify listen address
// (host and/or port).
func ServerListenOnAddr(t *testing.T, addr string) ServerOption {
	return func(server *httptest.Server) {
		t.Helper()

		if l := server.Listener; l != nil {
			l.Close()
		}

		l, err := net.Listen("tcp", addr)
		if err != nil {
			t.Fatal("net.Listen() error", err)
		}
		server.Listener = l
	}
}

// MakeServer creates a new server running a http.Handler.
func MakeServer(t *testing.T, h http.Handler, opts ...ServerOption) (host, port string, closeServer func()) {
	t.Helper()

	server := httptest.NewUnstartedServer(h)
	for i := range opts {
		opts[i](server)
	}

	host, port, err := net.SplitHostPort(server.Listener.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	closeServer = server.Close

	server.Start()

	return
}

// SendFile streams a file given by name to HTTP response.
func SendFile(t *testing.T, w http.ResponseWriter, file string) {
	f, err := os.Open(file)
	if err != nil {
		t.Error(err)
		return
	}
	defer f.Close()
	if _, err := io.Copy(w, f); err != nil {
		t.Error("Copy() error", err)
	}
}

// RespondStatus returns statusCodes in subsequent calls.
func RespondStatus(t *testing.T, statusCodes ...int) http.Handler {
	calls := atomic.NewInt32(-1)
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		idx := int(calls.Inc())
		if idx >= len(statusCodes) {
			t.Fatal("Too many requests statusCodes out of range")
		}
		w.WriteHeader(statusCodes[idx])
	})
}

// RespondHostStatus returns a fixed status based on target host.
func RespondHostStatus(t *testing.T, statusCode map[string]int) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		host, _, _ := net.SplitHostPort(r.Host)
		c, ok := statusCode[host]
		if !ok {
			t.Fatalf("Unexpected host %s", host)
		}
		w.WriteHeader(c)
	})
}
