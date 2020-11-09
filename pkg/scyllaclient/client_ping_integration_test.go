// Copyright (C) 2017 ScyllaDB

// +build all integration

package scyllaclient_test

import (
	"context"
	"net/http"
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/scylladb/go-log"
	"github.com/scylladb/scylla-manager/pkg/scyllaclient"
	. "github.com/scylladb/scylla-manager/pkg/testutils"
)

func TestCheckHostsConnectivityIntegration(t *testing.T) {
	client, err := scyllaclient.NewClient(scyllaclient.TestConfig(ManagedClusterHosts(), AgentAuthToken()), log.NewDevelopment())
	if err != nil {
		t.Fatal(err)
	}

	hosts := []string{ManagedClusterHost(), "xx.xx.xx.xx"}
	errs := client.CheckHostsConnectivity(context.Background(), hosts)

	t.Log(errs)

	if len(errs) != len(hosts) {
		t.Fatal("expected equal length")
	}
	if errs[0] != nil || errs[1] == nil {
		t.Fatal("expected <nil> and error")
	}
}

func TestClientClosestDCIntegration(t *testing.T) {
	client, err := scyllaclient.NewClient(scyllaclient.TestConfig(ManagedClusterHosts(), AgentAuthToken()), log.NewDevelopment())
	if err != nil {
		t.Fatal(err)
	}

	dcs := map[string][]string{
		"dc1": ManagedClusterHosts(),
		"xx":  {"xx.xx.xx.xx"},
	}

	closest, err := client.ClosestDC(context.Background(), dcs)
	if err != nil {
		t.Fatal(err)
	}
	if diff := cmp.Diff(closest, []string{"dc1", "xx"}); diff != "" {
		t.Fatal(closest, diff)
	}
}

func TestPingAuthIntegration(t *testing.T) {
	config := scyllaclient.TestConfig(ManagedClusterHosts(), "wrong auth token")

	client, err := scyllaclient.NewClient(config, log.NewDevelopment())
	if err != nil {
		t.Fatal(err)
	}

	ctx := context.Background()
	_, err = client.Ping(ctx, ManagedClusterHost(), 0)
	if scyllaclient.StatusCodeOf(err) != http.StatusUnauthorized {
		t.Error("expected 401 got", err)
	}
}
