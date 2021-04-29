// Copyright (C) 2017 ScyllaDB

package config_test

import (
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	"github.com/scylladb/go-log"
	"github.com/scylladb/scylla-manager/pkg/config"
	"github.com/scylladb/scylla-manager/pkg/service/backup"
	"github.com/scylladb/scylla-manager/pkg/service/healthcheck"
	"github.com/scylladb/scylla-manager/pkg/service/repair"
	"github.com/scylladb/scylla-manager/pkg/testutils"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

var serverConfigCmpOpts = cmp.Options{
	testutils.UUIDComparer(),
	cmpopts.IgnoreUnexported(config.DBConfig{}),
	cmpopts.IgnoreTypes(zap.AtomicLevel{}),
}

func TestConfigModification(t *testing.T) {
	t.Parallel()

	c, err := config.ParseServerConfigFiles([]string{"testdata/server/scylla-manager.yaml"})
	if err != nil {
		t.Fatal(err)
	}

	golden := config.ServerConfig{
		HTTP:          "127.0.0.1:80",
		HTTPS:         "127.0.0.1:443",
		TLSVersion:    "TLSv1.3",
		TLSCertFile:   "tls.cert",
		TLSKeyFile:    "tls.key",
		TLSCAFile:     "ca.cert",
		Prometheus:    "127.0.0.1:9090",
		Debug:         "127.0.0.1:112",
		Logger: config.LogConfig{
			Config: log.Config{
				Mode:  log.StderrMode,
				Level: zap.NewAtomicLevelAt(zapcore.DebugLevel),
			},
		},
		Database: config.DBConfig{
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
		SSL: config.SSLConfig{
			CertFile:     "ca.pem",
			Validate:     false,
			UserCertFile: "ssl.cert",
			UserKeyFile:  "ssl.key",
		},
		Healthcheck: healthcheck.Config{
			Timeout:    time.Second,
			SSLTimeout: time.Second,
			DynamicTimeout: healthcheck.DynamicTimeoutConfig{
				Enabled:          true,
				Probes:           500,
				StdDevMultiplier: 100,
				MaxTimeout:       1 * time.Minute,
			},
			NodeInfoTTL: time.Second,
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

	if diff := cmp.Diff(c, golden, serverConfigCmpOpts); diff != "" {
		t.Fatal(diff)
	}
}

func TestDefaultConfig(t *testing.T) {
	c, err := config.ParseServerConfigFiles([]string{"../../../dist/etc/scylla-manager/scylla-manager.yaml"})
	if err != nil {
		t.Fatal(err)
	}
	e := config.DefaultServerConfig()

	if diff := cmp.Diff(c, e, serverConfigCmpOpts, cmpopts.IgnoreFields(config.ServerConfig{}, "HTTP", "HTTPS")); diff != "" {
		t.Fatal(diff)
	}
}
