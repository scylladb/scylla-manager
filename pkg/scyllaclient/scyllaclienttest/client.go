// Copyright (C) 2017 ScyllaDB

package scyllaclienttest

import (
	"testing"

	"github.com/scylladb/go-log"
	"github.com/scylladb/scylla-manager/pkg/scyllaclient"
)

// TestHost should be used if a function in test requires host parameter.
const TestHost = "127.0.0.1"

// ClientOption allows to modify configuration in MakeClient.
type ClientOption func(*scyllaclient.Config)

// MakeClient creates a Client for testing. Typically host and port are set
// based on MakeServer result.
func MakeClient(t *testing.T, host, port string, opts ...ClientOption) *scyllaclient.Client {
	t.Helper()

	config := scyllaclient.DefaultConfig()
	config.Hosts = []string{host}
	config.Port = port
	config.Scheme = "http"

	for i := range opts {
		opts[i](&config)
	}

	client, err := scyllaclient.NewClient(config, log.NewDevelopment())
	if err != nil {
		t.Fatal(err)
	}
	return client
}
