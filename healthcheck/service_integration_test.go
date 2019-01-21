// Copyright (C) 2017 ScyllaDB

// +build all integration

package healthcheck

import (
	"context"
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	"github.com/scylladb/go-log"
	"github.com/scylladb/mermaid/cluster"
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
		func(ctx context.Context, id uuid.UUID) (*cluster.Cluster, error) {
			return &cluster.Cluster{ID: id}, nil
		},
		func(context.Context, uuid.UUID) (*scyllaclient.Client, error) {
			return scyllaclient.NewClient(ManagedClusterHosts, NewSSHTransport(), logger.Named("scylla"))
		},
		kv.NopStore{},
		kv.NopStore{},
		logger,
	)
	if err != nil {
		t.Fatal(err)
	}

	status, err := s.GetStatus(context.Background(), uuid.MustRandom())
	if err != nil {
		t.Fatal(err)
	}

	expected := []Status{
		{DC: "dc1", Host: "192.168.100.11", CQLStatus: "UP"},
		{DC: "dc1", Host: "192.168.100.12", CQLStatus: "UP"},
		{DC: "dc1", Host: "192.168.100.13", CQLStatus: "UP"},
		{DC: "dc2", Host: "192.168.100.21", CQLStatus: "UP"},
		{DC: "dc2", Host: "192.168.100.22", CQLStatus: "UP"},
		{DC: "dc2", Host: "192.168.100.23", CQLStatus: "UP"},
	}

	diffOpts := []cmp.Option{
		UUIDComparer(),
		cmpopts.IgnoreFields(Status{}, "RTT"),
	}
	if diff := cmp.Diff(status, expected, diffOpts...); diff != "" {
		t.Fatal(diff)
	}
}
