// Copyright (C) 2017 ScyllaDB

package server

import (
	"net"
	"net/url"
	"strings"
	"time"

	"github.com/pkg/errors"
	"github.com/scylladb/scylla-manager/pkg/config"
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

// Config contains configuration structure for scylla manager.
type Config struct {
	HTTP        string             `yaml:"http"`
	HTTPS       string             `yaml:"https"`
	TLSVersion  config.TLSVersion  `yaml:"tls_version"`
	TLSCertFile string             `yaml:"tls_cert_file"`
	TLSKeyFile  string             `yaml:"tls_key_file"`
	TLSCAFile   string             `yaml:"tls_ca_file"`
	Prometheus  string             `yaml:"prometheus"`
	Debug       string             `yaml:"debug"`
	Logger      config.LogConfig   `yaml:"logger"`
	Database    DBConfig           `yaml:"database"`
	SSL         SSLConfig          `yaml:"ssl"`
	Healthcheck healthcheck.Config `yaml:"healthcheck"`
	Backup      backup.Config      `yaml:"backup"`
	Repair      repair.Config      `yaml:"repair"`
}

func DefaultConfig() Config {
	return Config{
		TLSVersion: config.TLSv12,
		Prometheus: ":5090",
		Debug:      "127.0.0.1:5112",
		Logger:     DefaultLogConfig(),
		Database: DBConfig{
			Hosts:                         []string{"127.0.0.1"},
			Keyspace:                      "scylla_manager",
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
}

// ParseConfigFiles takes list of configuration file paths and returns parsed
// config struct with merged configuration from all provided files.
func ParseConfigFiles(files []string) (Config, error) {
	c := DefaultConfig()
	return c, cfgutil.ParseYAML(&c, files...)
}

func (c Config) Validate() error {
	if c.HTTP == "" && c.HTTPS == "" {
		return errors.New("missing http or https")
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

// HasTLSCert returns true iff TLSCertFile or TLSKeyFile is set.
func (c Config) HasTLSCert() bool {
	return c.TLSCertFile != "" || c.TLSKeyFile != ""
}

// BaseURL returns Scylla Manager base URL based on config.
func (c Config) BaseURL() string {
	const ipv4Zero, ipv6Zero1, ipv6Zero2 = "0.0.0.0", "::0", "::"
	const ipv4Localhost, ipv6Localhost = "127.0.0.1", "::1"

	var addr, scheme string
	if c.HTTP != "" {
		addr, scheme = c.HTTP, "http"
	} else {
		addr, scheme = c.HTTPS, "https"
	}
	if addr == "" {
		return ""
	}

	host, port, err := net.SplitHostPort(addr)
	if err != nil {
		return ""
	}

	switch host {
	case "":
		host = ipv4Localhost
	case ipv6Zero1, ipv6Zero2:
		host = ipv6Localhost
	case ipv4Zero:
		host = ipv4Localhost
	}

	return (&url.URL{
		Scheme: scheme,
		Host:   net.JoinHostPort(host, port),
		Path:   "/api/v1",
	}).String()
}

// Obfuscate returns Config with secrets replaced with ******.
func Obfuscate(c Config) Config {
	c.Database.Password = strings.Repeat("*", len(c.Database.Password))
	return c
}
