// Copyright (C) 2017 ScyllaDB

// +build all integration

package scyllaclient_test

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"testing"

	"github.com/scylladb/go-log"
	"github.com/scylladb/scylla-manager/pkg/scyllaclient"
	. "github.com/scylladb/scylla-manager/pkg/testutils"
)

func TestExecIntegration(t *testing.T) {
	client, err := scyllaclient.NewClient(scyllaclient.TestConfig(ManagedClusterHosts(), AgentAuthToken()), log.NewDevelopment())
	if err != nil {
		t.Fatal(err)
	}

	ctx := context.Background()

	hosts, err := client.Hosts(ctx)
	if err != nil {
		t.Fatal(err)
	}

	var buf bytes.Buffer
	for i := 0; i < 5; i++ {
		buf.WriteString(fmt.Sprint("echo ", i+1, "; sleep 1\n"))
	}
	if err := client.Exec(ctx, hosts, 0, &buf, os.Stdout); err != nil {
		t.Log(err)
	}
}
