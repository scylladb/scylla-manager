// Copyright (C) 2017 ScyllaDB

package main

import (
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	"github.com/scylladb/go-log"
	"github.com/scylladb/mermaid/pkg/service/backup"
	"github.com/scylladb/mermaid/pkg/service/healthcheck"
	repair "github.com/scylladb/mermaid/pkg/service/repair2"
	"github.com/scylladb/mermaid/pkg/testutils"
	"go.uber.org/zap/zapcore"
)

var serverConfigCmpOpts = cmp.Options{
	testutils.UUIDComparer(),
	cmpopts.IgnoreUnexported(dbConfig{}),
}

func TestConfigModification(t *testing.T) {
	t.Parallel()

	c, err := parseConfigFile([]string{"testdata/scylla-manager.yaml"})
	if err != nil {
		t.Fatal(err)
	}

	e := &serverConfig{
		HTTP:                     "127.0.0.1:80",
		HTTPS:                    "127.0.0.1:443",
		TLSCertFile:              "tls.cert",
		TLSKeyFile:               "tls.key",
		TLSCAFile:                "ca.cert",
		Prometheus:               "127.0.0.1:9090",
		PrometheusScrapeInterval: 1 * time.Second,
		Debug:                    "127.0.0.1:112",
		Logger: logConfig{
			Mode:  log.StderrMode,
			Level: zapcore.DebugLevel,
		},
		Database: dbConfig{
			Hosts:                         []string{"172.16.1.10", "172.16.1.20"},
			SSL:                           true,
			User:                          "user",
			Password:                      "password",
			LocalDC:                       "local",
			Keyspace:                      "scylla_manager",
			MigrateDir:                    "/etc/scylla-manager/cql",
			MigrateTimeout:                30 * time.Second,
			MigrateMaxWaitSchemaAgreement: 5 * time.Minute,
			ReplicationFactor:             3,
			Timeout:                       600 * time.Millisecond,
			TokenAware:                    false,
		},
		SSL: sslConfig{
			CertFile:     "ca.pem",
			Validate:     false,
			UserCertFile: "ssl.cert",
			UserKeyFile:  "ssl.key",
		},
		Healthcheck: healthcheck.Config{
			Timeout:    time.Second,
			SSLTimeout: time.Second,
		},
		Backup: backup.Config{
			DiskSpaceFreeMinPercent: 1,
			AgeMax:                  12 * time.Hour,
		},
		Repair: repair.Config{
			ErrorBackoff:          10 * time.Second,
			PollInterval:          500 * time.Millisecond,
			AgeMax:                12 * time.Hour,
			ShardingIgnoreMsbBits: 1,
		},
	}

	if diff := cmp.Diff(c, e, serverConfigCmpOpts); diff != "" {
		t.Fatal(diff)
	}
}

func TestDefaultConfig(t *testing.T) {
	c, err := parseConfigFile([]string{"../../../dist/etc/scylla-manager/scylla-manager.yaml"})
	if err != nil {
		t.Fatal(err)
	}
	e := defaultConfig()
	if diff := cmp.Diff(c, e, serverConfigCmpOpts, cmpopts.IgnoreFields(serverConfig{}, "HTTP", "HTTPS")); diff != "" {
		t.Fatal(diff)
	}
}

func TestValidateConfig(t *testing.T) {
	// TODO add validation tests
}
