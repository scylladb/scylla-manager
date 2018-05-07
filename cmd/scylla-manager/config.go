// Copyright (C) 2017 ScyllaDB

package main

import (
	"io/ioutil"
	"os"
	"time"

	"github.com/pkg/errors"
	log "github.com/scylladb/golog"
	"github.com/scylladb/mermaid/repair"
	"github.com/scylladb/mermaid/ssh"
	"go.uber.org/zap/zapcore"
	"gopkg.in/yaml.v2"
)

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
	Timeout                       time.Duration `yaml:"timeout"`
}

type serverConfig struct {
	HTTP        string `yaml:"http"`
	HTTPS       string `yaml:"https"`
	TLSCertFile string `yaml:"tls_cert_file"`
	TLSKeyFile  string `yaml:"tls_key_file"`

	Logger   log.Config    `yaml:"logger"`
	Database dbConfig      `yaml:"database"`
	SSH      ssh.Config    `yaml:"ssh"`
	Repair   repair.Config `yaml:"repair"`
}

func defaultConfig() *serverConfig {
	return &serverConfig{
		Logger: log.Config{
			Mode:  log.SyslogMode,
			Level: zapcore.InfoLevel,
		},
		Database: dbConfig{
			Keyspace:                      "scylla_manager",
			KeyspaceTplFile:               "/etc/scylla-manager/create_keyspace.cql.tpl",
			MigrateDir:                    "/etc/scylla-manager/cql",
			MigrateTimeout:                30 * time.Second,
			MigrateMaxWaitSchemaAgreement: 5 * time.Minute,
			ReplicationFactor:             1,
			Timeout:                       600 * time.Millisecond,
		},
		Repair: repair.DefaultConfig(),
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

	if c.SSH.User != "" {
		if err := c.SSH.Validate(); err != nil {
			return errors.Wrap(err, "ssh")
		}
	}
	if err := c.Repair.Validate(); err != nil {
		return errors.Wrap(err, "repair")
	}

	return nil
}
