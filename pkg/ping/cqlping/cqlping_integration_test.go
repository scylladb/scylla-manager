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
		Addr:     testutils.ManagedClusterHost() + ":9042",
		Timeout:  250 * time.Millisecond,
		Username: user,
		Password: password,
	}

	t.Run("simple", func(t *testing.T) {
		d, err := simplePing(context.Background(), config)
		if err != nil {
			t.Error(err)
		}
		t.Logf("simplePing() = %s", d)
	})

	t.Run("query", func(t *testing.T) {
		d, err := queryPing(context.Background(), config)
		if err != nil {
			t.Error(err)
		}
		t.Logf("queryPing() = %s", d)
	})

	t.Run("query wrong user", func(t *testing.T) {
		c := config
		c.Username = "foo"

		d, err := queryPing(context.Background(), c)
		if err != ping.ErrUnauthorised {
			t.Error("got", err, "expected", ping.ErrUnauthorised)
		}
		t.Logf("queryPing() = %s", d)
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
		d, err := simplePing(context.Background(), config)
		if err != nil {
			t.Error(err)
		}
		t.Logf("simplePing() = %s", d)
	})

	t.Run("query", func(t *testing.T) {
		d, err := queryPing(context.Background(), config)
		if err != nil {
			t.Error(err)
		}
		t.Logf("queryPing() = %s", d)
	})
}
