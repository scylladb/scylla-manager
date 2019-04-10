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
	"github.com/scylladb/go-log"
	"github.com/scylladb/mermaid/internal/osutil"
	. "github.com/scylladb/mermaid/mermaidtest"
	"github.com/scylladb/mermaid/uuid"
	"go.uber.org/zap"
	"go.uber.org/zap/zaptest/observer"
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
	core, o := observer.New(zap.InfoLevel)
	l := log.NewLogger(zap.New(core))
	initVer := "v1.0.0"
	hs := newMockHomeServer(initVer)
	defer hs.Close()
	macUUID := uuid.NewTime()
	regUUID := uuid.NewTime()
	dist := string(osutil.Centos)
	env := &mockEnv{macUUID, regUUID, dist, false}
	s := NewChecker(hs.ts.URL, initVer, l, env)

	t.Run("daily check", func(t *testing.T) {
		Print("When: latest version is same as installed version")
		hs.Reset()
		hs.Return(initVer)

		Print("When: and daily check is performed")
		s.CheckForUpdates(context.Background(), false)

		Print("Then: logs should be empty")
		logs := o.TakeAll()
		if len(logs) != 0 {
			t.Errorf("Expected no logs, got %+v", logs)
		}

		Print("And: home server should receive proper request params")
		got := hs.TakeAll()
		expected := []map[string]string{
			{"machine-uuid": macUUID.String(), "registration-uuid": regUUID.String(), "rtype": dist, "status": "md", "version": initVer},
		}
		if diff := cmp.Diff(got, expected); diff != "" {
			t.Fatal(diff)
		}
	})

	t.Run("install check", func(t *testing.T) {
		Print("When: latest version is same as installed version")
		hs.Reset()
		hs.Return(initVer)

		Print("When: and install check is performed")
		s.CheckForUpdates(context.Background(), true)

		Print("Then: logs should be empty")
		logs := o.TakeAll()
		if len(logs) != 0 {
			t.Errorf("Expected no logs, got %+v", logs)
		}

		Print("And: home server should receive proper request params")
		got := hs.TakeAll()
		expected := []map[string]string{
			{"machine-uuid": macUUID.String(), "registration-uuid": regUUID.String(), "rtype": dist, "status": "mi", "version": initVer},
		}
		if diff := cmp.Diff(got, expected); diff != "" {
			t.Fatal(diff)
		}
	})

	t.Run("daily check version outdated", func(t *testing.T) {
		Print("When: latest version is ahead of installed version")
		hs.Reset()
		hs.Return("v1.0.1")

		Print("When: and daily check is performed")
		s.CheckForUpdates(context.Background(), false)

		Print("Then: logs should have an entry about version check")
		logs := o.TakeAll()
		if len(logs) != 1 || logs[0].Level != zap.InfoLevel {
			t.Errorf("Expected warning entry, got %+v", logs)
		}

		Print("And: home server should receive proper request params")
		got := hs.TakeAll()
		expected := []map[string]string{
			{"machine-uuid": macUUID.String(), "registration-uuid": regUUID.String(), "rtype": dist, "status": "md", "version": initVer},
		}
		if diff := cmp.Diff(got, expected); diff != "" {
			t.Fatal(diff)
		}
	})

	t.Run("daily check version ahead", func(t *testing.T) {
		Print("When: latest version is behind installed version")
		hs.Reset()
		hs.Return("v0.9.1")

		Print("When: and daily check is performed")
		s.CheckForUpdates(context.Background(), false)

		Print("Then: logs should be empty")
		logs := o.TakeAll()
		if len(logs) != 0 {
			t.Errorf("Expected no logs, got %+v", logs)
		}

		Print("And: home server should receive proper request params")
		got := hs.TakeAll()
		expected := []map[string]string{
			{"machine-uuid": macUUID.String(), "registration-uuid": regUUID.String(), "rtype": dist, "status": "md", "version": initVer},
		}
		if diff := cmp.Diff(got, expected); diff != "" {
			t.Fatal(diff)
		}
	})

	t.Run("install check version outdated", func(t *testing.T) {
		Print("When: latest version ahead of installed version")
		hs.Reset()
		hs.Return("v1.0.1")

		Print("When: and install check is performed")
		s.CheckForUpdates(context.Background(), true)

		Print("Then: logs should have an entry about version check")
		logs := o.TakeAll()
		if len(logs) != 1 || logs[0].Level != zap.InfoLevel {
			t.Errorf("Expected warning entry, got %+v", logs)
		}

		Print("And: home server should receive proper request params")
		got := hs.TakeAll()
		expected := []map[string]string{
			{"machine-uuid": macUUID.String(), "registration-uuid": regUUID.String(), "rtype": dist, "status": "mi", "version": initVer},
		}
		if diff := cmp.Diff(got, expected); diff != "" {
			t.Fatal(diff)
		}
	})

	t.Run("daily check docker", func(t *testing.T) {
		env.docker = true
		Print("When: latest version is same as installed version")
		hs.Reset()
		hs.Return(initVer)

		Print("When: and daily check is performed")
		s.CheckForUpdates(context.Background(), false)

		Print("Then: logs should be empty")
		logs := o.TakeAll()
		if len(logs) != 0 {
			t.Errorf("Expected no logs, got %+v", logs)
		}

		Print("And: home server should receive proper request params")
		got := hs.TakeAll()
		expected := []map[string]string{
			{"machine-uuid": macUUID.String(), "registration-uuid": regUUID.String(), "rtype": dist, "status": "mdd", "version": initVer},
		}
		if diff := cmp.Diff(got, expected); diff != "" {
			t.Fatal(diff)
		}
	})

	t.Run("install check docker", func(t *testing.T) {
		env.docker = true
		Print("When: latest version is same as installed version")
		hs.Reset()
		hs.Return(initVer)

		Print("When: and install check is performed")
		s.CheckForUpdates(context.Background(), true)

		Print("Then: logs should be empty")
		logs := o.TakeAll()
		if len(logs) != 0 {
			t.Errorf("Expected no logs, got %+v", logs)
		}

		Print("And: home server should receive proper request params")
		got := hs.TakeAll()
		expected := []map[string]string{
			{"machine-uuid": macUUID.String(), "registration-uuid": regUUID.String(), "rtype": dist, "status": "mdi", "version": initVer},
		}
		if diff := cmp.Diff(got, expected); diff != "" {
			t.Fatal(diff)
		}
	})
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
