// Copyright (C) 2017 ScyllaDB

package main

import (
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	"github.com/scylladb/go-log"
	"github.com/scylladb/mermaid/healthcheck"
	"github.com/scylladb/mermaid/internal/ssh"
	"github.com/scylladb/mermaid/mermaidtest"
	"github.com/scylladb/mermaid/repair"
	"go.uber.org/zap/zapcore"
)

func TestConfigModification(t *testing.T) {
	t.Parallel()

	c, err := newConfigFromFile("testdata/scylla-manager.yaml")
	if err != nil {
		t.Fatal(err)
	}

	e := &serverConfig{
		HTTP:        "127.0.0.1:80",
		HTTPS:       "127.0.0.1:443",
		TLSCertFile: "tls.cert",
		TLSKeyFile:  "tls.key",
		TLSCAFile:   "ca.cert",
		Prometheus:  "127.0.0.1:9090",
		Gops:        ":56112",
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
			KeyspaceTplFile:               "/etc/scylla-manager/create_keyspace.cql.tpl",
			MigrateDir:                    "/etc/scylla-manager/cql",
			MigrateTimeout:                30 * time.Second,
			MigrateMaxWaitSchemaAgreement: 5 * time.Minute,
			ReplicationFactor:             3,
			Timeout:                       600 * time.Millisecond,
		},
		SSL: sslConfig{
			CertFile:     "ca.pem",
			Validate:     false,
			UserCertFile: "ssl.cert",
			UserKeyFile:  "ssl.key",
		},
		SSH: ssh.Config{
			Port:                22000,
			ServerAliveInterval: 15 * time.Second,
			ServerAliveCountMax: 3,
		},
		Healthcheck: healthcheck.Config{
			Timeout:    time.Second,
			SSLTimeout: time.Second,
		},
		Repair: repair.Config{
			SegmentsPerRepair:      7,
			SegmentTokensMax:       10,
			ShardParallelMax:       10,
			ShardFailedSegmentsMax: 0,
			ErrorBackoff:           10 * time.Second,
			PollInterval:           500 * time.Millisecond,
			AgeMax:                 12 * time.Hour,
			ShardingIgnoreMsbBits:  1,
		},
	}

	if diff := cmp.Diff(c, e, mermaidtest.UUIDComparer()); diff != "" {
		t.Fatal(diff)
	}
}

func TestDefaultConfig(t *testing.T) {
	c, err := newConfigFromFile("../../dist/etc/scylla-manager.yaml")
	if err != nil {
		t.Fatal(err)
	}
	e := defaultConfig()
	opts := []cmp.Option{mermaidtest.UUIDComparer(), cmpopts.IgnoreTypes(serverConfig{})}
	if diff := cmp.Diff(c, e, opts...); diff != "" {
		t.Fatal(diff)
	}
}

func TestValidateConfig(t *testing.T) {
	// TODO add validation tests
}
