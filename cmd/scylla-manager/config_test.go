// Copyright (C) 2017 ScyllaDB

package main

import (
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	log "github.com/scylladb/golog"
	"github.com/scylladb/mermaid/mermaidtest"
	"github.com/scylladb/mermaid/repair"
	"github.com/scylladb/mermaid/ssh"
	"go.uber.org/zap/zapcore"
)

func TestNewConfigFromFile(t *testing.T) {
	t.Parallel()

	c, err := newConfigFromFile("testdata/scylla-manager.yml")
	if err != nil {
		t.Fatal(err)
	}

	e := &serverConfig{
		HTTP:        "127.0.0.1:80",
		HTTPS:       "127.0.0.1:443",
		TLSCertFile: "tls.cert",
		TLSKeyFile:  "tls.key",
		Logger: log.Config{
			Mode:  log.StderrMode,
			Level: zapcore.DebugLevel,
		},
		Database: dbConfig{
			Hosts:                         []string{"172.16.1.10", "172.16.1.20"},
			User:                          "user",
			Password:                      "password",
			Keyspace:                      "scylla_manager",
			KeyspaceTplFile:               "/etc/scylla-manager/create_keyspace.cql.tpl",
			MigrateDir:                    "/etc/scylla-manager/cql",
			MigrateTimeout:                30 * time.Second,
			MigrateMaxWaitSchemaAgreement: 5 * time.Minute,
			ReplicationFactor:             3,
			Timeout:                       600 * time.Millisecond,
		},
		SSH: ssh.Config{
			User:         "user",
			IdentityFile: "identity_file",
		},
		Repair: repair.Config{
			SegmentSizeLimit:      10,
			SegmentsPerRepair:     7,
			SegmentErrorLimit:     0,
			StopOnError:           true,
			ErrorBackoff:          10 * time.Second,
			PollInterval:          500 * time.Millisecond,
			AutoScheduleDelay:     100 * time.Second,
			MaxRunAge:             12 * time.Hour,
			ShardingIgnoreMsbBits: 1,
		},
	}

	if diff := cmp.Diff(c, e, mermaidtest.UUIDComparer()); diff != "" {
		t.Fatal(diff)
	}
}

func TestValidateConfig(t *testing.T) {
	// TODO add validation tests
}
