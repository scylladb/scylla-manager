// Copyright (C) 2017 ScyllaDB

// +build all integration

package scyllaclient_test

import (
	"context"
	"testing"

	"github.com/scylladb/go-log"
	. "github.com/scylladb/mermaid/mermaidtest"
	"github.com/scylladb/mermaid/scyllaclient"
)

func registerRemoteWithEnvAuth(t *testing.T, c *scyllaclient.Client, host string) {
	t.Helper()

	if err := c.RcloneRegisterS3Remote(context.Background(), host, testRemote, NewS3Params()); err != nil {
		t.Fatal(err)
	}
}

func TestRcloneListDirAgentIntegration(t *testing.T) {
	config := scyllaclient.DefaultConfig()
	config.Hosts = ManagedClusterHosts
	config.Transport = NewSSHTransport()

	testHost := ManagedClusterHosts[0]

	client, err := scyllaclient.NewClient(config, log.NewDevelopment())
	if err != nil {
		t.Fatal(err)
	}

	S3InitBucket(t, testBucket)
	registerRemoteWithEnvAuth(t, client, testHost)

	ctx := context.Background()

	d, err := client.RcloneListDir(ctx, testHost, remotePath(""), false)
	if err != nil {
		t.Fatal(err)
	}
	if len(d) > 0 {
		t.Errorf("Expected bucket to be empty, got: len(files)=%d", len(d))
	}
}
