// Copyright (C) 2017 ScyllaDB

package server_test

import (
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	"github.com/scylladb/go-log"
	"github.com/scylladb/scylla-manager/pkg/config"
	"github.com/scylladb/scylla-manager/pkg/config/server"
	"github.com/scylladb/scylla-manager/pkg/service/backup"
	"github.com/scylladb/scylla-manager/pkg/service/healthcheck"
	"github.com/scylladb/scylla-manager/pkg/service/repair"
	"github.com/scylladb/scylla-manager/pkg/testutils"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

var configCmpOpts = cmp.Options{
	testutils.UUIDComparer(),
	cmpopts.IgnoreUnexported(server.DBConfig{}),
	cmpopts.IgnoreTypes(zap.AtomicLevel{}),
}

func TestConfigModification(t *testing.T) {
	t.Parallel()

	c, err := server.ParseConfigFiles([]string{"testdata/scylla-manager.yaml"})
	if err != nil {
		t.Fatal(err)
	}

	golden := server.Config{
		HTTP:        "127.0.0.1:80",
		HTTPS:       "127.0.0.1:443",
		TLSVersion:  "TLSv1.3",
		TLSCertFile: "tls.cert",
		TLSKeyFile:  "tls.key",
		TLSCAFile:   "ca.cert",
		Prometheus:  "127.0.0.1:9090",
		Debug:       "127.0.0.1:112",
		Logger: config.LogConfig{
			Config: log.Config{
				Mode:  log.StderrMode,
				Level: zap.NewAtomicLevelAt(zapcore.DebugLevel),
			},
		},
		Database: server.DBConfig{
			Hosts:                         []string{"172.16.1.10", "172.16.1.20"},
			SSL:                           true,
			User:                          "user",
			Password:                      "password",
			LocalDC:                       "local",
			Keyspace:                      "scylla_manager",
			MigrateTimeout:                30 * time.Second,
			MigrateMaxWaitSchemaAgreement: 5 * time.Minute,
			ReplicationFactor:             3,
			Timeout:                       600 * time.Millisecond,
			TokenAware:                    false,
		},
		SSL: server.SSLConfig{
			CertFile:     "ca.pem",
			Validate:     false,
			UserCertFile: "ssl.cert",
			UserKeyFile:  "ssl.key",
		},
		Healthcheck: healthcheck.Config{
			RelativeTimeout: time.Second,
			MaxTimeout:      1 * time.Minute,
			Probes:          500,
			NodeInfoTTL:     time.Second,
		},
		Backup: backup.Config{
			DiskSpaceFreeMinPercent:   1,
			LongPollingTimeoutSeconds: 5,
			AgeMax:                    12 * time.Hour,
		},
		Repair: repair.Config{
			PollInterval:                    500 * time.Millisecond,
			LongPollingTimeoutSeconds:       5,
			AgeMax:                          12 * time.Hour,
			GracefulStopTimeout:             60 * time.Second,
			ForceRepairType:                 repair.TypeAuto,
			Murmur3PartitionerIgnoreMSBBits: 12,
		},
	}

	if diff := cmp.Diff(c, golden, configCmpOpts); diff != "" {
		t.Fatal(diff)
	}
}

func TestDefaultConfig(t *testing.T) {
	c, err := server.ParseConfigFiles([]string{"../../../dist/etc/scylla-manager.yaml"})
	if err != nil {
		t.Fatal(err)
	}
	e := server.DefaultConfig()

	if diff := cmp.Diff(c, e, configCmpOpts, cmpopts.IgnoreFields(server.Config{}, "HTTP", "HTTPS")); diff != "" {
		t.Fatal(diff)
	}
}

func TestBaseURL(t *testing.T) {
	table := []struct {
		Name   string
		Config server.Config
		Golden string
	}{
		{
			Name:   "empty configuration",
			Config: server.Config{},
			Golden: "",
		},
		{
			Name:   "HTTP",
			Config: server.Config{HTTP: "127.0.0.1:12345"},
			Golden: "http://127.0.0.1:12345/api/v1",
		},
		{
			Name:   "HTTPS",
			Config: server.Config{HTTPS: "127.0.0.1:54321"},
			Golden: "https://127.0.0.1:54321/api/v1",
		},
		{
			Name:   "HTTP override",
			Config: server.Config{HTTP: "127.0.0.1:12345", HTTPS: "127.0.0.2:54321"},
			Golden: "http://127.0.0.1:12345/api/v1",
		},
		{
			Name:   "HTTP override on all interfaces",
			Config: server.Config{HTTP: "0.0.0.0:12345", HTTPS: "127.0.0.1:54321"},
			Golden: "http://127.0.0.1:12345/api/v1",
		},
		{
			Name:   "HTTP empty host",
			Config: server.Config{HTTP: ":12345"},
			Golden: "http://127.0.0.1:12345/api/v1",
		},
		{
			Name:   "IPV6",
			Config: server.Config{HTTP: "[::1]:12345"},
			Golden: "http://[::1]:12345/api/v1",
		},
		{
			Name:   "IPV6 on all interfaces",
			Config: server.Config{HTTPS: "[::0]:54321"},
			Golden: "https://[::1]:54321/api/v1",
		},
	}

	for i := range table {
		test := table[i]
		t.Run(test.Name, func(t *testing.T) {
			if diff := cmp.Diff(test.Golden, test.Config.BaseURL()); diff != "" {
				t.Error(diff)
			}
		})
	}
}
