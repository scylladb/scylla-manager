// Copyright (C) 2017 ScyllaDB

package scyllaclienttest

import (
	"encoding/json"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path"
	"strconv"
	"strings"
	"testing"

	agentModels "github.com/scylladb/scylla-manager/v3/swagger/gen/agent/models"
	"go.uber.org/atomic"
)

// ServerOption allows to modify test Server before it's started.
type ServerOption func(*httptest.Server)

// ServerListenOnAddr is ServerOption that allows to specify listen address
// (host and/or port).
func ServerListenOnAddr(t *testing.T, addr string) ServerOption {
	t.Helper()
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
	t.Helper()
	f, err := os.Open(file)
	if err != nil {
		t.Error(err)
		return
	}
	defer f.Close()

	statusCode := statusCodeFromFile(file)
	if statusCode != 0 {
		w.WriteHeader(statusCode)
	}

	if _, err := io.Copy(w, f); err != nil {
		t.Error("Copy() error", err)
	}
}

func statusCodeFromFile(file string) (statusCode int) {
	s := strings.Split(path.Base(file), ".")
	if len(s) != 3 {
		return
	}
	i, err := strconv.Atoi(s[1])
	if err != nil {
		return
	}
	statusCode = i
	return
}

// RespondStatus returns statusCodes in subsequent calls.
func RespondStatus(t *testing.T, statusCodes ...int) http.Handler {
	t.Helper()
	calls := atomic.NewInt32(-1)
	return http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		idx := int(calls.Inc())
		if idx >= len(statusCodes) {
			t.Fatal("Too many requests statusCodes out of range")
		}
		writeAgentErrorHeader(t, w, statusCodes[idx])
	})
}

// RespondHostStatus returns a fixed status based on target host.
func RespondHostStatus(t *testing.T, statusCode map[string]int) http.Handler {
	t.Helper()
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		host, _, _ := net.SplitHostPort(r.Host)
		c, ok := statusCode[host]
		if !ok {
			t.Fatalf("Unexpected host %s", host)
		}
		writeAgentErrorHeader(t, w, c)
	})
}

func writeAgentErrorHeader(t *testing.T, w http.ResponseWriter, statusCode int) {
	t.Helper()
	w.WriteHeader(statusCode)
	if statusCode >= 400 {
		if err := json.NewEncoder(w).Encode(agentModels.ErrorResponse{Status: int64(statusCode)}); err != nil {
			t.Fatal("Unexpected Encode error", err)
		}
	}
}
