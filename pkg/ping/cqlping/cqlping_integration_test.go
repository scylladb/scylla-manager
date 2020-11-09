// Copyright (C) 2017 ScyllaDB

// +build all integration

package cqlping

import (
	"context"
	"crypto/tls"
	"testing"
	"time"

	"github.com/scylladb/scylla-manager/pkg/testutils"
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
