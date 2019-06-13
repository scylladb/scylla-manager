// Copyright (C) 2017 ScyllaDB

// +build all integration

package healthcheck

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	"github.com/pkg/errors"
	"github.com/scylladb/go-log"
	"github.com/scylladb/mermaid/internal/kv"
	. "github.com/scylladb/mermaid/mermaidtest"
	"github.com/scylladb/mermaid/scyllaclient"
	"github.com/scylladb/mermaid/uuid"
	"go.uber.org/zap/zapcore"
)

func TestGetStatusIntegration(t *testing.T) {
	logger := log.NewDevelopmentWithLevel(zapcore.InfoLevel).Named("healthcheck")

	s, err := NewService(
		DefaultConfig(),
		func(ctx context.Context, id uuid.UUID) (string, error) {
			return "test_cluster", nil
		},
		func(context.Context, uuid.UUID) (*scyllaclient.Client, error) {
			return scyllaclient.NewClient(scyllaclient.DefaultConfigWithHosts(ManagedClusterHosts), logger.Named("scylla"))
		},
		kv.NopStore{},
		kv.NopStore{},
		logger,
	)
	if err != nil {
		t.Fatal(err)
	}

	compare := func(t *testing.T, got, expected []Status) {
		t.Helper()
		diffOpts := []cmp.Option{
			UUIDComparer(),
			cmpopts.IgnoreFields(Status{}, "CQLRtt", "RESTRtt"),
		}
		if diff := cmp.Diff(got, expected, diffOpts...); diff != "" {
			t.Error(diff)
		}
	}

	t.Run("all nodes UP", func(t *testing.T) {
		status, err := s.GetStatus(context.Background(), uuid.MustRandom())
		if err != nil {
			t.Error(err)
			return
		}

		expected := []Status{
			{DC: "dc1", Host: "192.168.100.11", CQLStatus: "UP", RESTStatus: "UP"},
			{DC: "dc1", Host: "192.168.100.12", CQLStatus: "UP", RESTStatus: "UP"},
			{DC: "dc1", Host: "192.168.100.13", CQLStatus: "UP", RESTStatus: "UP"},
			{DC: "dc2", Host: "192.168.100.21", CQLStatus: "UP", RESTStatus: "UP"},
			{DC: "dc2", Host: "192.168.100.22", CQLStatus: "UP", RESTStatus: "UP"},
			{DC: "dc2", Host: "192.168.100.23", CQLStatus: "UP", RESTStatus: "UP"},
		}
		compare(t, status, expected)
	})

	t.Run("node_12 REST DOWN", func(t *testing.T) {
		host := "192.168.100.12"
		blockREST(t, host)
		defer unblockREST(t, host)

		status, err := s.GetStatus(context.Background(), uuid.MustRandom())
		if err != nil {
			t.Error(err)
			return
		}

		expected := []Status{
			{DC: "dc1", Host: "192.168.100.11", CQLStatus: "UP", RESTStatus: "UP"},
			{DC: "dc1", Host: "192.168.100.12", CQLStatus: "UP", RESTStatus: "DOWN"},
			{DC: "dc1", Host: "192.168.100.13", CQLStatus: "UP", RESTStatus: "UP"},
			{DC: "dc2", Host: "192.168.100.21", CQLStatus: "UP", RESTStatus: "UP"},
			{DC: "dc2", Host: "192.168.100.22", CQLStatus: "UP", RESTStatus: "UP"},
			{DC: "dc2", Host: "192.168.100.23", CQLStatus: "UP", RESTStatus: "UP"},
		}
		compare(t, status, expected)
	})

	t.Run("node_12 CQL DOWN", func(t *testing.T) {
		host := "192.168.100.12"
		blockCQL(t, host)
		defer unblockCQL(t, host)

		status, err := s.GetStatus(context.Background(), uuid.MustRandom())
		if err != nil {
			t.Error(err)
			return
		}

		expected := []Status{
			{DC: "dc1", Host: "192.168.100.11", CQLStatus: "UP", RESTStatus: "UP"},
			{DC: "dc1", Host: "192.168.100.12", CQLStatus: "DOWN", RESTStatus: "UP"},
			{DC: "dc1", Host: "192.168.100.13", CQLStatus: "UP", RESTStatus: "UP"},
			{DC: "dc2", Host: "192.168.100.21", CQLStatus: "UP", RESTStatus: "UP"},
			{DC: "dc2", Host: "192.168.100.22", CQLStatus: "UP", RESTStatus: "UP"},
			{DC: "dc2", Host: "192.168.100.23", CQLStatus: "UP", RESTStatus: "UP"},
		}
		compare(t, status, expected)
	})

	t.Run("managed cluster nodes REST DOWN", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		for i := range ManagedClusterHosts {
			blockREST(t, ManagedClusterHosts[i])
			defer unblockREST(t, ManagedClusterHosts[i])
		}

		_, err := s.GetStatus(ctx, uuid.MustRandom())
		if err == nil {
			t.Error("Expected error got nil")
		} else if !strings.Contains(err.Error(), "failed to get dcs for cluster") {
			t.Errorf("Expected 'failed to get dcs for cluster' got %s", err)
		}
	})

	t.Run("context timeout exceeded", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond)
		defer cancel()

		_, err := s.GetStatus(ctx, uuid.MustRandom())
		if err == nil {
			t.Error("Expected error got nil")
		}
	})
}

func blockREST(t *testing.T, h string) {
	t.Helper()
	if err := block(h, CmdBlockScyllaREST); err != nil {
		t.Error(err)
	}
}

func unblockREST(t *testing.T, h string) {
	t.Helper()
	if err := unblock(h, CmdUnblockScyllaREST); err != nil {
		t.Error(err)
	}
}

func blockCQL(t *testing.T, h string) {
	t.Helper()
	if err := block(h, CmdBlockScyllaCQL); err != nil {
		t.Error(err)
	}
}

func unblockCQL(t *testing.T, h string) {
	t.Helper()
	if err := unblock(h, CmdUnblockScyllaCQL); err != nil {
		t.Error(err)
	}
}

func block(h, cmd string) error {
	stdout, stderr, err := ExecOnHost(h, cmd)
	if err != nil {
		return errors.Wrapf(err, "block failed host: %s, stdout %s, stderr %s", h, stdout, stderr)
	}
	return nil
}

func unblock(h, cmd string) error {
	stdout, stderr, err := ExecOnHost(h, cmd)
	if err != nil {
		return errors.Wrapf(err, "unblock failed host: %s, stdout %s, stderr %s", h, stdout, stderr)
	}
	return nil
}
