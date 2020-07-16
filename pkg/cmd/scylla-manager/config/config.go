// Copyright (C) 2017 ScyllaDB

package config

import (
	"time"

	"github.com/pkg/errors"
	"github.com/scylladb/go-log"
	"github.com/scylladb/mermaid/pkg/service/backup"
	"github.com/scylladb/mermaid/pkg/service/healthcheck"
	"github.com/scylladb/mermaid/pkg/service/repair"
	"github.com/scylladb/mermaid/pkg/util/cfgutil"
	"go.uber.org/zap/zapcore"
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
	MigrateDir                    string        `yaml:"migrate_dir"`
	MigrateTimeout                time.Duration `yaml:"migrate_timeout"`
	MigrateMaxWaitSchemaAgreement time.Duration `yaml:"migrate_max_wait_schema_agreement"`
	ReplicationFactor             int           `yaml:"replication_factor"`
	Timeout                       time.Duration `yaml:"timeout"`
	TokenAware                    bool          `yaml:"token_aware"`
	// initAddr specifies address used to create manager keyspace and tables.
	initAddr string
}

func (dbc *dbConfig) SetInitAddr(initAddr string) {
	dbc.initAddr = initAddr
}

func (dbc *dbConfig) InitAddr() string {
	return dbc.initAddr
}

type sslConfig struct {
	CertFile     string `yaml:"cert_file"`
	Validate     bool   `yaml:"validate"`
	UserCertFile string `yaml:"user_cert_file"`
	UserKeyFile  string `yaml:"user_key_file"`
}

// ServerConfig contains configuration structure for scylla manager.
type ServerConfig struct {
	HTTP                     string             `yaml:"http"`
	HTTPS                    string             `yaml:"https"`
	TLSCertFile              string             `yaml:"tls_cert_file"`
	TLSKeyFile               string             `yaml:"tls_key_file"`
	TLSCAFile                string             `yaml:"tls_ca_file"`
	Prometheus               string             `yaml:"prometheus"`
	PrometheusScrapeInterval time.Duration      `yaml:"prometheus_scrape_interval"`
	Debug                    string             `json:"debug"`
	Logger                   logConfig          `yaml:"logger"`
	Database                 dbConfig           `yaml:"database"`
	SSL                      sslConfig          `yaml:"ssl"`
	Healthcheck              healthcheck.Config `yaml:"healthcheck"`
	Backup                   backup.Config      `yaml:"backup"`
	Repair                   repair.Config      `yaml:"repair"`
}

func defaultConfig() *ServerConfig {
	config := &ServerConfig{
		TLSCertFile:              "/var/lib/scylla-manager/scylla_manager.crt",
		TLSKeyFile:               "/var/lib/scylla-manager/scylla_manager.key",
		Prometheus:               ":5090",
		PrometheusScrapeInterval: 5 * time.Second,
		Debug:                    "127.0.0.1:5112",
		Logger: logConfig{
			Mode:        log.StderrMode,
			Level:       zapcore.InfoLevel,
			Development: false,
		},
		Database: dbConfig{
			Hosts:                         []string{"127.0.0.1"},
			Keyspace:                      "scylla_manager",
			MigrateDir:                    "/etc/scylla-manager/cql",
			MigrateTimeout:                30 * time.Second,
			MigrateMaxWaitSchemaAgreement: 5 * time.Minute,
			ReplicationFactor:             1,
			Timeout:                       600 * time.Millisecond,
			TokenAware:                    true,
		},
		SSL: sslConfig{
			Validate: true,
		},
		Healthcheck: healthcheck.DefaultConfig(),
		Backup:      backup.DefaultConfig(),
		Repair:      repair.DefaultConfig(),
	}

	return config
}

// ParseConfigFile takes list of configuration file paths and returns parsed
// config struct with merged configuration from all provided files.
func ParseConfigFile(files []string) (*ServerConfig, error) {
	c := defaultConfig()
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

	if c.PrometheusScrapeInterval <= 0 {
		return errors.New("prometheus_scrape_interval must be > 0")
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
