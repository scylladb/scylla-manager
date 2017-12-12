// Copyright (C) 2017 ScyllaDB

package main

import (
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/scylladb/mermaid/mermaidtest"
	"github.com/scylladb/mermaid/uuid"
)

func TestNewConfigFromFile(t *testing.T) {
	t.Parallel()

	c, err := newConfigFromFile("testdata/scylla-mgmt.yml")
	if err != nil {
		t.Fatal(err)
	}

	var u uuid.UUID
	u.UnmarshalText([]byte("a3f1b32b-ed5b-438d-81a7-c82eb7bde800"))

	e := &serverConfig{
		HTTP:        "127.0.0.1:80",
		HTTPS:       "127.0.0.1:443",
		TLSCertFile: "tls.cert",
		TLSKeyFile:  "tls.key",
		Database: dbConfig{
			Hosts:                         []string{"172.16.1.10", "172.16.1.20"},
			User:                          "user",
			Password:                      "password",
			Keyspace:                      "scylla_management",
			KeyspaceTplFile:               "/etc/scylla-mgmt/create_keyspace.cql.tpl",
			MigrateDir:                    "/etc/scylla-mgmt/cql",
			MigrateTimeout:                30 * time.Second,
			MigrateMaxWaitSchemaAgreement: 5 * time.Minute,
			ReplicationFactor:             3,
			Timeout:                       600 * time.Millisecond,
		},
		SSH: sshConfig{
			User:           "user",
			IdentityFile:   "identity_file",
			KnownHostsFile: "known_hosts",
		},
	}

	if diff := cmp.Diff(c, e, mermaidtest.UUIDComparer()); diff != "" {
		t.Fatal(diff)
	}
}

func TestValidateConfig(t *testing.T) {
	// TODO add validation tests
}
