// Copyright (C) 2017 ScyllaDB

package config

import (
	"time"

	"github.com/pkg/errors"
	"github.com/scylladb/scylla-manager/pkg/service/backup"
	"github.com/scylladb/scylla-manager/pkg/service/healthcheck"
	"github.com/scylladb/scylla-manager/pkg/service/repair"
	"github.com/scylladb/scylla-manager/pkg/util/cfgutil"
)

// DBConfig specifies Scylla Manager backend database configuration options.
type DBConfig struct {
	Hosts                         []string      `yaml:"hosts"`
	SSL                           bool          `yaml:"ssl"`
	User                          string        `yaml:"user"`
	Password                      string        `yaml:"password"`
	LocalDC                       string        `yaml:"local_dc"`
	Keyspace                      string        `yaml:"keyspace"`
	MigrateDir                    string        `yaml:"migrate_dir"`
	MigrateTimeout                time.Duration `yaml:"migrate_timeout"`
	MigrateMaxWaitSchemaAgreement time.Duration `yaml:"migrate_max_wait_schema_agreement"`
	ReplicationFactor             int           `yaml:"replication_factor"`
	Timeout                       time.Duration `yaml:"timeout"`
	TokenAware                    bool          `yaml:"token_aware"`

	// InitAddr specifies address used to create manager keyspace and tables.
	InitAddr string
}

// SSLConfig specifies Scylla Manager backend database SSL configuration options.
type SSLConfig struct {
	CertFile     string `yaml:"cert_file"`
	Validate     bool   `yaml:"validate"`
	UserCertFile string `yaml:"user_cert_file"`
	UserKeyFile  string `yaml:"user_key_file"`
}

// ServerConfig contains configuration structure for scylla manager.
type ServerConfig struct {
	HTTP          string             `yaml:"http"`
	HTTPS         string             `yaml:"https"`
	TLSCertFile   string             `yaml:"tls_cert_file"`
	TLSKeyFile    string             `yaml:"tls_key_file"`
	TLSCAFile     string             `yaml:"tls_ca_file"`
	Prometheus    string             `yaml:"prometheus"`
	Debug         string             `yaml:"debug"`
	SwaggerUIPath string             `yaml:"swagger_ui_path"`
	Logger        LogConfig          `yaml:"logger"`
	Database      DBConfig           `yaml:"database"`
	SSL           SSLConfig          `yaml:"ssl"`
	Healthcheck   healthcheck.Config `yaml:"healthcheck"`
	Backup        backup.Config      `yaml:"backup"`
	Repair        repair.Config      `yaml:"repair"`
}

func DefaultServerConfig() *ServerConfig {
	config := &ServerConfig{
		TLSCertFile:   "/var/lib/scylla-manager/scylla_manager.crt",
		TLSKeyFile:    "/var/lib/scylla-manager/scylla_manager.key",
		Prometheus:    ":5090",
		Debug:         "127.0.0.1:5112",
		SwaggerUIPath: "/var/lib/scylla-manager/swagger-ui",
		Logger:        DefaultLogConfig(),
		Database: DBConfig{
			Hosts:                         []string{"127.0.0.1"},
			Keyspace:                      "scylla_manager",
			MigrateDir:                    "/etc/scylla-manager/cql",
			MigrateTimeout:                30 * time.Second,
			MigrateMaxWaitSchemaAgreement: 5 * time.Minute,
			ReplicationFactor:             1,
			Timeout:                       600 * time.Millisecond,
			TokenAware:                    true,
		},
		SSL: SSLConfig{
			Validate: true,
		},
		Healthcheck: healthcheck.DefaultConfig(),
		Backup:      backup.DefaultConfig(),
		Repair:      repair.DefaultConfig(),
	}

	return config
}

// ParseServerConfigFiles takes list of configuration file paths and returns parsed
// config struct with merged configuration from all provided files.
func ParseServerConfigFiles(files []string) (*ServerConfig, error) {
	c := DefaultServerConfig()
	return c, cfgutil.ParseYAML(c, files...)
}

// Validate checks if config contains correct values.
func (c *ServerConfig) Validate() error {
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
	if err := c.Backup.Validate(); err != nil {
		return errors.Wrap(err, "backup")
	}
	if err := c.Repair.Validate(); err != nil {
		return errors.Wrap(err, "repair")
	}

	return nil
}
