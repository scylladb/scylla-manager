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
	table := []struct {
		Name string

		InstalledVersion string
		LatestVersion    string
		Install          bool
		Docker           bool

		Result Result
		Status string
	}{
		{
			Name:             "daily equal version",
			InstalledVersion: "v1.0.0-0.20200123.7cf18f6b",
			LatestVersion:    "v1.0.0",
			Install:          false,
			Docker:           false,
			Result: Result{
				UpdateAvailable: false,
				Installed:       "v1.0.0-0.20200123.7cf18f6b",
				Available:       "v1.0.0",
			},
			Status: "md",
		}, {
			Name:             "daily install equal version",
			InstalledVersion: "v1.0.0-0.20200123.7cf18f6b",
			LatestVersion:    "v1.0.0",
			Install:          true,
			Docker:           false,
			Result: Result{
				UpdateAvailable: false,
				Installed:       "v1.0.0-0.20200123.7cf18f6b",
				Available:       "v1.0.0",
			},
			Status: "mi",
		}, {
			Name:             "docker equal version",
			InstalledVersion: "v1.0.0-0.20200123.7cf18f6b",
			LatestVersion:    "v1.0.0",
			Install:          false,
			Docker:           true,
			Result: Result{
				UpdateAvailable: false,
				Installed:       "v1.0.0-0.20200123.7cf18f6b",
				Available:       "v1.0.0",
			},
			Status: "mdd",
		}, {
			Name:             "docker install equal version",
			InstalledVersion: "v1.0.0-0.20200123.7cf18f6b",
			LatestVersion:    "v1.0.0",
			Install:          true,
			Docker:           true,
			Result: Result{
				UpdateAvailable: false,
				Installed:       "v1.0.0-0.20200123.7cf18f6b",
				Available:       "v1.0.0",
			},
			Status: "mdi",
		}, {
			Name:             "version outdated",
			InstalledVersion: "v1.0.0-0.20200123.7cf18f6b",
			LatestVersion:    "v1.0.1",
			Result: Result{
				UpdateAvailable: true,
				Installed:       "v1.0.0-0.20200123.7cf18f6b",
				Available:       "v1.0.1",
			},
			Status: "md",
		}, {
			Name:             "check version ahead",
			InstalledVersion: "v1.0.0-0.20200123.7cf18f6b",
			LatestVersion:    "v0.9.1",
			Result: Result{
				UpdateAvailable: false,
				Installed:       "v1.0.0-0.20200123.7cf18f6b",
				Available:       "v0.9.1",
			},
			Status: "md",
		}, {
			Name:             "master version",
			InstalledVersion: "666.dev-0.20200902.a6a8ce8e",
			LatestVersion:    "v0.9.1",
			Result: Result{
				UpdateAvailable: false,
				Installed:       "666.dev-0.20200902.a6a8ce8e",
				Available:       "v0.9.1",
			},
			Status: "md",
		}, {
			Name:             "long master version",
			InstalledVersion: "666.development-0.20200325.9fee712d62",
			LatestVersion:    "v0.9.1",
			Result: Result{
				UpdateAvailable: false,
				Installed:       "666.development-0.20200325.9fee712d62",
				Available:       "v0.9.1",
			},
			Status: "md",
		}, {
			Name:             "master version with v prefix",
			InstalledVersion: "v666.dev-0.20200902.a6a8ce8e",
			LatestVersion:    "v0.9.1",
			Result: Result{
				UpdateAvailable: false,
				Installed:       "v666.dev-0.20200902.a6a8ce8e",
				Available:       "v0.9.1",
			},
			Status: "md",
		}, {
			Name:             "long master version with v prefix",
			InstalledVersion: "v666.development-0.20200325.9fee712d62",
			LatestVersion:    "v0.9.1",
			Result: Result{
				UpdateAvailable: false,
				Installed:       "v666.development-0.20200325.9fee712d62",
				Available:       "v0.9.1",
			},
			Status: "md",
		},
		{
			Name:             "master version available",
			InstalledVersion: "v666.dev-0.20200325.9fee712d62",
			LatestVersion:    "v666.dev-0.20200325.9fee712d62",
			Result: Result{
				UpdateAvailable: false,
				Installed:       "v666.dev-0.20200325.9fee712d62",
				Available:       "v666.dev-0.20200325.9fee712d62",
			},
			Status: "md",
		},
		{
			Name:             "rc version when non-rc available",
			InstalledVersion: "2.2.rc1",
			LatestVersion:    "2.2.0",
			Result: Result{
				UpdateAvailable: true,
				Installed:       "2.2.rc1",
				Available:       "2.2.0",
			},
			Status: "md",
		},
		{
			Name:             "rc version when non-rc older available",
			InstalledVersion: "2.2.rc1",
			LatestVersion:    "2.1.0",
			Result: Result{
				UpdateAvailable: false,
				Installed:       "2.2.rc1",
				Available:       "2.1.0",
			},
			Status: "md",
		},
	}

	for _, test := range table {
		t.Run(test.Name, func(t *testing.T) {
			hs := newMockHomeServer(test.InstalledVersion)
			defer hs.Close()

			macUUID := uuid.NewTime()
			regUUID := uuid.NewTime()
			dist := string(osutil.Centos)
			env := &mockEnv{macUUID, regUUID, dist, test.Docker}
			s := NewChecker(hs.ts.URL, test.InstalledVersion, env)

			Print("When: latest: " + test.LatestVersion)
			hs.Reset()
			hs.Return(test.LatestVersion)

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

			params := map[string]string{"machine-uuid": macUUID.String(), "registration-uuid": regUUID.String(), "rtype": dist, "status": test.Status, "version": test.InstalledVersion}
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
