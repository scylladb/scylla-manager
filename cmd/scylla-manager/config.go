// Copyright (C) 2017 ScyllaDB

package main

import (
	"io/ioutil"
	"time"

	"github.com/pkg/errors"
	"github.com/scylladb/go-log"
	"github.com/scylladb/mermaid/healthcheck"
	"github.com/scylladb/mermaid/internal/ssh"
	"github.com/scylladb/mermaid/repair"
	"go.uber.org/zap/zapcore"
	"gopkg.in/yaml.v2"
)

type logConfig struct {
	Mode        log.Mode      `yaml:"mode"`
	Level       zapcore.Level `yaml:"level"`
	Development bool          `yaml:"development"`
}

type dbConfig struct {
	Hosts                         []string      `yaml:"hosts"`
	SSL                           bool          `yaml:"ssl"`
	User                          string        `yaml:"user"`
	Password                      string        `yaml:"password"`
	LocalDC                       string        `yaml:"local_dc"`
	Keyspace                      string        `yaml:"keyspace"`
	KeyspaceTplFile               string        `yaml:"keyspace_tpl_file"`
	MigrateDir                    string        `yaml:"migrate_dir"`
	MigrateTimeout                time.Duration `yaml:"migrate_timeout"`
	MigrateMaxWaitSchemaAgreement time.Duration `yaml:"migrate_max_wait_schema_agreement"`
	ReplicationFactor             int           `yaml:"replication_factor"`
	Timeout                       time.Duration `yaml:"timeout"`
}

type sslConfig struct {
	CertFile     string `yaml:"cert_file"`
	Validate     bool   `yaml:"validate"`
	UserCertFile string `yaml:"user_cert_file"`
	UserKeyFile  string `yaml:"user_key_file"`
}

type serverConfig struct {
	HTTP        string             `yaml:"http"`
	HTTPS       string             `yaml:"https"`
	TLSCertFile string             `yaml:"tls_cert_file"`
	TLSKeyFile  string             `yaml:"tls_key_file"`
	Prometheus  string             `yaml:"prometheus"`
	Gops        string             `json:"gops"`
	Logger      logConfig          `yaml:"logger"`
	Database    dbConfig           `yaml:"database"`
	SSL         sslConfig          `yaml:"ssl"`
	SSH         ssh.Config         `yaml:"ssh"`
	Healthcheck healthcheck.Config `yaml:"healthcheck"`
	Repair      repair.Config      `yaml:"repair"`
}

func defaultConfig() *serverConfig {
	return &serverConfig{
		Prometheus: ":56090",
		Gops:       ":56112",
		Logger: logConfig{
			Mode:        log.SyslogMode,
			Level:       zapcore.InfoLevel,
			Development: false,
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
		SSL: sslConfig{
			Validate: true,
		},
		SSH:         ssh.DefaultConfig(),
		Healthcheck: healthcheck.DefaultConfig(),
		Repair:      repair.DefaultConfig(),
	}
}

func newConfigFromFile(filename ...string) (*serverConfig, error) {
	config := defaultConfig()

	for _, f := range filename {
		b, err := ioutil.ReadFile(f)
		if err != nil {
			return nil, errors.Wrapf(err, "failed to read file %q", f)
		}
		if err := yaml.Unmarshal(b, config); err != nil {
			return nil, errors.Wrapf(err, "failed to parse file %q", f)
		}
	}
	return config, nil
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

	if err := c.SSH.Validate(); err != nil {
		return errors.Wrap(err, "ssh")
	}

	if err := c.Repair.Validate(); err != nil {
		return errors.Wrap(err, "repair")
	}

	return nil
}
