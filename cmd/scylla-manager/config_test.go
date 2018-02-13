// Copyright (C) 2017 ScyllaDB

package main

import (
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/scylladb/mermaid/mermaidtest"
	"github.com/scylladb/mermaid/repair"
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
		SSH: sshConfig{
			User:         "user",
			IdentityFile: "identity_file",
		},
		Repair: repair.Config{
			SegmentsPerRepair: 7,
			SegmentSizeLimit:  10,
			SegmentErrorLimit: 0,
			PollInterval:      500 * time.Millisecond,
			ErrorBackoff:      10 * time.Second,
			AutoScheduleDelay: 100 * time.Second,
			MaxRunAge:         12 * time.Hour,
		},
	}

	if diff := cmp.Diff(c, e, mermaidtest.UUIDComparer()); diff != "" {
		t.Fatal(diff)
	}
}

func TestValidateConfig(t *testing.T) {
	// TODO add validation tests
}
