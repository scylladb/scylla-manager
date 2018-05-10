// Copyright (C) 2017 ScyllaDB

// +build all integration

package scyllaclient

import (
	"context"
	"testing"

	log "github.com/scylladb/golog"
	"github.com/scylladb/mermaid/internal/ssh"
	"github.com/scylladb/mermaid/mermaidtest"
)

func TestSSHTransportIntegration(t *testing.T) {
	client, err := NewClient(mermaidtest.ManagedClusterHosts, ssh.NewDevelopmentTransport(), log.NewDevelopment())
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()

	for i := 0; i < 10; i++ {
		_, err := client.Keyspaces(ctx)
		if err != nil {
			t.Fatal(err)
		}
	}
}
