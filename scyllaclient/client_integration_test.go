// Copyright (C) 2017 ScyllaDB

// +build all integration

package scyllaclient

import (
	"context"
	"testing"

	"github.com/scylladb/mermaid/log"
	"github.com/scylladb/mermaid/mermaidtest"
	"github.com/scylladb/mermaid/ssh"
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
