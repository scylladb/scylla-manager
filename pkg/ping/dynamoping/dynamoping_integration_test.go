// Copyright (C) 2017 ScyllaDB

//go:build all || integration

package dynamoping

import (
	"context"
	"testing"
	"time"

	"github.com/pkg/errors"
	"github.com/scylladb/scylla-manager/v3/pkg/testutils/testconfig"

	_ "github.com/scylladb/scylla-manager/v3/pkg/testutils"
)

func TestPingIntegration(t *testing.T) {
	config := Config{
		Addr:                   "http://" + testconfig.ManagedClusterHost() + ":8000",
		Timeout:                250 * time.Millisecond,
		RequiresAuthentication: true,
	}
	if testconfig.IsSSLEnabled() {
		config.Addr = "https://" + testconfig.ManagedClusterHost() + ":8100"
		sslOpts := testconfig.CQLSSLOptions()
		tlsConfig, err := testconfig.TLSConfig(sslOpts)
		if err != nil {
			t.Fatalf("setup tls config: %v", err)
		}
		config.TLSConfig = tlsConfig
	}

	t.Run("simple", func(t *testing.T) {
		d, err := SimplePing(context.Background(), config)
		if err != nil {
			t.Error(err)
		}
		t.Logf("simplePing() = %s", d)
	})

	t.Run("query", func(t *testing.T) {
		d, err := QueryPing(context.Background(), config)
		if err != nil {
			if errors.Is(err, ErrAlternatorQueryPingNotSupported) {
				t.Skip(ErrAlternatorQueryPingNotSupported)
			}
			t.Error(err)
		}
		t.Logf("QueryPing() = %s", d)
	})
}
