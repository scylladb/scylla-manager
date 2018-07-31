// Copyright (C) 2017 ScyllaDB

// +build all integration

package cqlping

import (
	"context"
	"testing"

	"github.com/scylladb/mermaid/mermaidtest"
)

func TestPingIntegration(t *testing.T) {
	_, err := Ping(context.Background(), mermaidtest.ManagedClusterHosts[0])
	if err != nil {
		t.Fatal(err)
	}
}
