// Copyright (C) 2017 ScyllaDB

//go:build all || integration
// +build all integration

package cqlping

import (
	"context"
	"crypto/tls"
	"testing"
	"time"

	"github.com/scylladb/scylla-manager/v3/pkg/ping"
	"github.com/scylladb/scylla-manager/v3/pkg/testutils"
)

func TestPingIntegration(t *testing.T) {
	user, password := testutils.ManagedClusterCredentials()
	config := Config{
		Addr:    testutils.ManagedClusterHost() + ":9042",
		Timeout: 250 * time.Millisecond,
	}

	t.Run("simple", func(t *testing.T) {
		d, err := NativeCQLPing(context.Background(), config)
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
		Addr:    testutils.ManagedClusterHost() + ":9042",
		Timeout: 250 * time.Millisecond,
		TLSConfig: &tls.Config{
			InsecureSkipVerify: true,
		},
	}

	t.Run("simple", func(t *testing.T) {
		d, err := NativeCQLPing(context.Background(), config)
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
