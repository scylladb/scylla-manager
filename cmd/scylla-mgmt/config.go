// Copyright (C) 2017 ScyllaDB

package main

import (
	"io/ioutil"
	"os"
	"time"

	"github.com/pkg/errors"
	"github.com/scylladb/mermaid/uuid"
	"gopkg.in/yaml.v2"
)

// clusterConfig is a temporary solution and will be soon replaced by a
// a cluster configuration service.
type clusterConfig struct {
	UUID                            uuid.UUID `yaml:"uuid"`
	Hosts                           []string  `yaml:"hosts"`
	ShardCount                      float64   `yaml:"shard_count"`
	Murmur3PartitionerIgnoreMsbBits float64   `yaml:"murmur3_partitioner_ignore_msb_bits"`
}

type dbConfig struct {
	Hosts                         []string      `yaml:"hosts"`
	User                          string        `yaml:"user"`
	Password                      string        `yaml:"password"`
	Keyspace                      string        `yaml:"keyspace"`
	KeyspaceTplFile               string        `yaml:"keyspace_tpl_file"`
	MigrateDir                    string        `yaml:"migrate_dir"`
	MigrateTimeout                time.Duration `yaml:"migrate_timeout"`
	MigrateMaxWaitSchemaAgreement time.Duration `yaml:"migrate_max_wait_schema_agreement"`
	ReplicationFactor             int           `yaml:"replication_factor"`
}

type serverConfig struct {
	HTTP        string   `yaml:"http"`
	HTTPS       string   `yaml:"https"`
	TLSCertFile string   `yaml:"tls_cert_file"`
	TLSKeyFile  string   `yaml:"tls_key_file"`
	Database    dbConfig `yaml:"database"`

	Clusters []*clusterConfig `yaml:"clusters"`
}

func defaultConfig() *serverConfig {
	return &serverConfig{
		Database: dbConfig{
			Keyspace:                      "scylla_management",
			KeyspaceTplFile:               "/etc/scylla-mgmt/create_keyspace.cql.tpl",
			MigrateDir:                    "/etc/scylla-mgmt/cql",
			MigrateTimeout:                30 * time.Second,
			MigrateMaxWaitSchemaAgreement: 5 * time.Minute,
			ReplicationFactor:             1,
		},
	}
}

func newConfigFromFile(file string) (*serverConfig, error) {
	f, err := os.Open(file)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	b, err := ioutil.ReadAll(f)
	if err != nil {
		return nil, err
	}

	config := defaultConfig()
	return config, yaml.Unmarshal(b, config)
}

func (c *serverConfig) validate() error {
	if c.HTTP == "" && c.HTTPS == "" {
		return errors.New("missing http or https")
	}
	if c.HTTPS != "" {
		if c.TLSCertFile == "" {
			return errors.New("missing tls_cert_file")
		}
		if c.TLSKeyFile == "" {
			return errors.New("missing tls_key_file")
		}
	}

	if len(c.Database.Hosts) == 0 {
		return errors.New("missing database.hosts")
	}
	if c.Database.ReplicationFactor <= 0 {
		return errors.New("invalid database.replication_factor <= 0")
	}

	for _, cluster := range c.Clusters {
		if len(cluster.Hosts) == 0 {
			errors.Errorf("cluster %s: missing %q", cluster.UUID, "hosts")
		}
		if cluster.ShardCount == 0 {
			errors.Errorf("cluster %s: missing %q", cluster.UUID, "shard_count")
		}
		if cluster.Murmur3PartitionerIgnoreMsbBits == 0 {
			errors.Errorf("cluster %s: missing %q", cluster.UUID, "murmur3_partitioner_ignore_msb_bits")
		}
	}

	return nil
}
