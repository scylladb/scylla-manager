// Copyright (C) 2017 ScyllaDB

// +build all integration

package scyllaclient

import (
	"context"
	"testing"

	"github.com/scylladb/go-log"
	"github.com/scylladb/mermaid/internal/ssh"
	"github.com/scylladb/mermaid/mermaidtest"
)

func TestClientSSHTransportIntegration(t *testing.T) {
	client, err := NewClient(mermaidtest.ManagedClusterHosts, ssh.NewDevelopmentTransport(), log.NewDevelopment())
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 10; i++ {
		_, err := client.Keyspaces(context.Background())
		if err != nil {
			t.Fatal(err)
		}
	}
}

func TestClientClosestDCIntegration(t *testing.T) {
	client, err := NewClient(mermaidtest.ManagedClusterHosts, ssh.NewDevelopmentTransport(), log.NewDevelopment())
	if err != nil {
		t.Fatal(err)
	}

	dcs := map[string][]string{
		"dc1": mermaidtest.ManagedClusterHosts,
		"xx":  {"xx.xx.xx.xx"},
	}

	dc, err := client.ClosestDC(context.Background(), dcs)
	if err != nil {
		t.Fatal(err)
	}
	if dc != "dc1" {
		t.Fatalf("expected %s, got %s", "dc1", dc)
	}
}
