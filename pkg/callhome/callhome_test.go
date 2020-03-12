// Copyright (C) 2017 ScyllaDB

package callhome

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"

	"github.com/google/go-cmp/cmp"
	. "github.com/scylladb/mermaid/pkg/testutils"
	"github.com/scylladb/mermaid/pkg/util/osutil"
	"github.com/scylladb/mermaid/pkg/util/uuid"
)

type mockEnv struct {
	macUUID uuid.UUID
	regUUID uuid.UUID
	dist    string
	docker  bool
}

func (e *mockEnv) MacUUID() uuid.UUID {
	return e.macUUID
}

func (e *mockEnv) RegUUID() uuid.UUID {
	return e.regUUID
}

func (e *mockEnv) LinuxDistro() string {
	return e.dist
}

func (e *mockEnv) Docker() bool {
	return e.docker
}

func TestChecker(t *testing.T) {
	initVer := "v1.0.0-0.20200123.7cf18f6b"
	semanticVer := "v1.0.0"

	hs := newMockHomeServer(initVer)
	defer hs.Close()

	macUUID := uuid.NewTime()
	regUUID := uuid.NewTime()
	dist := string(osutil.Centos)
	env := &mockEnv{macUUID, regUUID, dist, false}
	s := NewChecker(hs.ts.URL, initVer, env)
	params := map[string]string{"machine-uuid": macUUID.String(), "registration-uuid": regUUID.String(), "rtype": dist, "status": "md", "version": initVer}

	table := []struct {
		Name    string
		Version string
		Install bool
		Result  Result
		Status  string
	}{
		{
			Name:    "daily check",
			Version: initVer,
			Install: false,
			Result: Result{
				UpdateAvailable: false,
				Installed:       semanticVer,
				Available:       initVer,
			},
			Status: "md",
		}, {
			Name:    "install check",
			Version: initVer,
			Install: true,
			Result: Result{
				UpdateAvailable: false,
				Installed:       semanticVer,
				Available:       initVer,
			},
			Status: "mi",
		}, {
			Name:    "daily check version outdated",
			Version: "v1.0.1",
			Install: false,
			Result: Result{
				UpdateAvailable: true,
				Installed:       semanticVer,
				Available:       "v1.0.1",
			},
			Status: "md",
		}, {
			Name:    "daily check version ahead",
			Version: "v0.9.1",
			Install: false,
			Result: Result{
				UpdateAvailable: false,
				Installed:       semanticVer,
				Available:       "v0.9.1",
			},
			Status: "md",
		}, {
			Name:    "install check version outdated",
			Version: "v1.0.1",
			Install: true,
			Result: Result{
				UpdateAvailable: true,
				Installed:       semanticVer,
				Available:       "v1.0.1",
			},
			Status: "mi",
		}, {
			Name:    "daily check version equal",
			Version: "v1.0.0",
			Install: false,
			Result: Result{
				UpdateAvailable: false,
				Installed:       semanticVer,
				Available:       "v1.0.0",
			},
			Status: "md",
		}, {
			Name:    "daily check docker",
			Version: initVer,
			Install: false,
			Result: Result{
				UpdateAvailable: false,
				Installed:       semanticVer,
				Available:       initVer,
			},
			Status: "md",
		}, {
			Name:    "install check docker",
			Version: initVer,
			Install: true,
			Result: Result{
				UpdateAvailable: false,
				Installed:       semanticVer,
				Available:       initVer,
			},
			Status: "mi",
		},
	}

	for _, test := range table {
		t.Run(test.Name, func(t *testing.T) {
			Print("When: latest: " + test.Version)
			hs.Reset()
			hs.Return(test.Version)

			Print("And: daily check is performed")
			res, err := s.CheckForUpdates(context.Background(), test.Install)
			if err != nil {
				t.Fatal(err)
			}
			Print("Then: result is expected")
			if diff := cmp.Diff(res, test.Result); diff != "" {
				t.Error(diff)
			}

			Print("And: home server should receive proper request params")
			got := hs.TakeAll()
			params["status"] = test.Status
			if diff := cmp.Diff(got, []map[string]string{params}); diff != "" {
				t.Fatal(diff)
			}
		})
	}
}

type mockHomeServer struct {
	ts *httptest.Server

	mu      sync.Mutex
	calls   []map[string]string
	version string
}

func newMockHomeServer(version string) *mockHomeServer {
	hs := &mockHomeServer{
		version: version,
	}
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hs.mu.Lock()
		defer hs.mu.Unlock()

		call := map[string]string{
			"status":            r.FormValue("sts"),
			"machine-uuid":      r.FormValue("uu"),
			"version":           r.FormValue("version"),
			"registration-uuid": r.FormValue("rid"),
			"rtype":             r.FormValue("rtype"),
		}

		hs.calls = append(hs.calls, call)

		out, err := json.Marshal(checkResponse{Version: hs.version, LatestPatchVersion: hs.version})
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		w.Header().Set("Content-Type", "application/json")
		w.Write(out)
	}))
	hs.ts = ts
	return hs
}

func (hs *mockHomeServer) Close() {
	hs.ts.Close()
}

func (hs *mockHomeServer) TakeAll() []map[string]string {
	hs.mu.Lock()
	defer hs.mu.Unlock()
	calls := hs.calls
	return calls
}

func (hs *mockHomeServer) Reset() {
	hs.mu.Lock()
	defer hs.mu.Unlock()
	hs.calls = nil
}

func (hs *mockHomeServer) Return(version string) {
	hs.mu.Lock()
	defer hs.mu.Unlock()
	hs.version = version
}
