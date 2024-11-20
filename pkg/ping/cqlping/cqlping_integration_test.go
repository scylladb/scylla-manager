// Copyright (C) 2017 ScyllaDB

//go:build all || integration
// +build all integration

package cqlping

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/gocql/gocql"
	"github.com/pkg/errors"
	"github.com/scylladb/scylla-manager/v3/pkg/testutils/testconfig"

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

	sslEnabled, err := strconv.ParseBool(os.Getenv("SSL_ENABLED"))
	if err != nil {
		t.Fatalf("parse SSL_ENABLED env var: %v\n", err)
	}

	sessionHosts, err := cluster.GetRPCAddresses(context.Background(), client, []string{testconfig.ManagedClusterHost()}, !sslEnabled)
	if err != nil {
		t.Fatal(err)
	}
	user, password := testconfig.ManagedClusterCredentials()
	config := Config{
		Addr:    sessionHosts[0],
		Timeout: 250 * time.Millisecond,
	}

	if sslEnabled {
		testSSLConfig := testconfig.CQLSSLOptions()
		tlsConfig, err := setupTLSConfig(testSSLConfig)
		if err != nil {
			t.Fatalf("setup tls config: %v", err)
		}
		config.TLSConfig = tlsConfig
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

// setupTLSConfig copied from github.com/gocql/gocql/connectionpool.go to convert gocql.SslOptions into *tls.Config
func setupTLSConfig(sslOpts *gocql.SslOptions) (*tls.Config, error) {
	//  Config.InsecureSkipVerify | EnableHostVerification | Result
	//  Config is nil             | true                   | verify host
	//  Config is nil             | false                  | do not verify host
	//  false                     | false                  | verify host
	//  true                      | false                  | do not verify host
	//  false                     | true                   | verify host
	//  true                      | true                   | verify host
	var tlsConfig *tls.Config
	if sslOpts.Config == nil {
		tlsConfig = &tls.Config{
			InsecureSkipVerify: !sslOpts.EnableHostVerification,
		}
	} else {
		// use clone to avoid race.
		tlsConfig = sslOpts.Config.Clone()
	}

	if tlsConfig.InsecureSkipVerify && sslOpts.EnableHostVerification {
		tlsConfig.InsecureSkipVerify = false
	}

	// ca cert is optional
	if sslOpts.CaPath != "" {
		if tlsConfig.RootCAs == nil {
			tlsConfig.RootCAs = x509.NewCertPool()
		}

		pem, err := os.ReadFile(sslOpts.CaPath)
		if err != nil {
			return nil, fmt.Errorf("connectionpool: unable to open CA certs: %v", err)
		}

		if !tlsConfig.RootCAs.AppendCertsFromPEM(pem) {
			return nil, errors.New("connectionpool: failed parsing or CA certs")
		}
	}

	if sslOpts.CertPath != "" || sslOpts.KeyPath != "" {
		mycert, err := tls.LoadX509KeyPair(sslOpts.CertPath, sslOpts.KeyPath)
		if err != nil {
			return nil, fmt.Errorf("connectionpool: unable to load X509 key pair: %v", err)
		}
		tlsConfig.Certificates = append(tlsConfig.Certificates, mycert)
	}

	return tlsConfig, nil
}
