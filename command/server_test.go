// Copyright (C) 2017 ScyllaDB

package command

import (
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/scylladb/mermaid/uuid"
)

func TestServerReadConfig(t *testing.T) {
	t.Parallel()

	s := ServerCommand{}
	c, err := s.readConfig("testdata/scylla-mgmt.yml")
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
			Hosts:    []string{"172.16.1.10", "172.16.1.20"},
			Keyspace: "scylla_management",
			User:     "user",
			Password: "password",
		},
		Clusters: []*clusterConfig{{UUID: u, Hosts: []string{"172.16.1.10", "172.16.1.20"}}},
	}

	if diff := cmp.Diff(c, e, cmp.AllowUnexported(uuid.UUID{})); diff != "" {
		t.Fatal(diff)
	}
}
