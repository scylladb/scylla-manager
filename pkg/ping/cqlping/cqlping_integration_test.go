// Copyright (C) 2017 ScyllaDB

//go:build all || integration
// +build all integration

package cqlping

import (
	"context"
	"crypto/tls"
	"github.com/scylladb/scylla-manager/v3/pkg/testutils/testconfig"
	"testing"
	"time"

	"github.com/scylladb/go-log"
	"github.com/scylladb/scylla-manager/v3/pkg/ping"
	"github.com/scylladb/scylla-manager/v3/pkg/scyllaclient"
	"github.com/scylladb/scylla-manager/v3/pkg/service/cluster"
	"github.com/scylladb/scylla-manager/v3/pkg/testutils"
	"go.uber.org/zap/zapcore"
)

func TestPingIntegration(t *testing.T) {
	client := newTestClient(t, log.NewDevelopmentWithLevel(zapcore.InfoLevel).Named("client"), nil)
	defer client.Close()

	sessionHosts, err := cluster.GetRPCAddresses(context.Background(), client, []string{testconfig.ManagedClusterHost()})
	if err != nil {
		t.Fatal(err)
	}

	user, password := testconfig.ManagedClusterCredentials()
	config := Config{
		Addr:    sessionHosts[0],
		Timeout: 250 * time.Millisecond,
	}

	t.Run("simple", func(t *testing.T) {
		d, err := NativeCQLPing(context.Background(), config, log.NopLogger)
		if err != nil {
			t.Error(err)
		}
		t.Logf("NativeCQLPing() = %s", d)
	})

	t.Run("query", func(t *testing.T) {
		d, err := QueryPing(context.Background(), config, user, password)
		if err != nil {
			t.Error(err)
		}
		t.Logf("QueryPing() = %s", d)
	})

	t.Run("query wrong user", func(t *testing.T) {
		c := config

		d, err := QueryPing(context.Background(), c, "foo", password)
		if err != ping.ErrUnauthorised {
			t.Error("got", err, "expected", ping.ErrUnauthorised)
		}
		t.Logf("QueryPing() = %s", d)
	})

}

func TestPingTLSIntegration(t *testing.T) {
	t.SkipNow()

	config := Config{
		Addr:    testconfig.ManagedClusterHost() + ":9042",
		Timeout: 250 * time.Millisecond,
		TLSConfig: &tls.Config{
			InsecureSkipVerify: true,
		},
	}

	t.Run("simple", func(t *testing.T) {
		d, err := NativeCQLPing(context.Background(), config, log.NopLogger)
		if err != nil {
			t.Error(err)
		}
		t.Logf("NativeCQLPing() = %s", d)
	})

	t.Run("query", func(t *testing.T) {
		d, err := QueryPing(context.Background(), config, "", "")
		if err != nil {
			t.Error(err)
		}
		t.Logf("QueryPing() = %s", d)
	})
}

func newTestClient(t *testing.T, logger log.Logger, config *scyllaclient.Config) *scyllaclient.Client {
	t.Helper()

	if config == nil {
		c := scyllaclient.TestConfig(testconfig.ManagedClusterHosts(), testutils.AgentAuthToken())
		config = &c
	}

	c, err := scyllaclient.NewClient(*config, logger)
	if err != nil {
		t.Fatal(err)
	}
	return c
}
