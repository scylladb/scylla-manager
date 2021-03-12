// Copyright (C) 2017 ScyllaDB

package config

import (
	"os"
	"strings"

	"github.com/pkg/errors"
	"github.com/scylladb/scylla-manager/pkg/rclone"
	"github.com/scylladb/scylla-manager/pkg/util/cfgutil"
	"go.uber.org/multierr"
)

const (
	// DefaultAgentHTTPSPort specifies the port Agent should start listening
	// on for HTTPS requests if not explicitly specified.
	DefaultAgentHTTPSPort = "10001"
	// NoCPU is a cpuset marker indicating that no CPU was selected for pinning.
	NoCPU = -1
)

// ScyllaConfig contains selected elements of Scylla configuration.
type ScyllaConfig struct {
	APIAddress string `yaml:"api_address"`
	APIPort    string `yaml:"api_port"`

	ListenAddress     string
	PrometheusAddress string
	PrometheusPort    string
	DataDirectory     string
}

func (c ScyllaConfig) Validate() (errs error) {
	if c.APIAddress == "" {
		errs = multierr.Append(errs, errors.New("missing api_address"))
	}
	if c.APIPort == "" {
		errs = multierr.Append(errs, errors.New("missing api_port"))
	}
	return
}

// AgentConfig specifies the agent and scylla configuration.
type AgentConfig struct {
	AuthToken   string              `yaml:"auth_token"`
	HTTPS       string              `yaml:"https"`
	TLSCertFile string              `yaml:"tls_cert_file"`
	TLSKeyFile  string              `yaml:"tls_key_file"`
	Prometheus  string              `yaml:"prometheus"`
	Debug       string              `yaml:"debug"`
	CPU         int                 `yaml:"cpu"`
	Logger      LogConfig           `yaml:"logger"`
	Scylla      ScyllaConfig        `yaml:"scylla"`
	S3          rclone.S3Options    `yaml:"s3"`
	GCS         rclone.GCSOptions   `yaml:"gcs"`
	Azure       rclone.AzureOptions `yaml:"azure"`
}

func DefaultAgentConfig() AgentConfig {
	return AgentConfig{
		TLSCertFile: "/var/lib/scylla-manager/scylla_manager.crt",
		TLSKeyFile:  "/var/lib/scylla-manager/scylla_manager.key",
		Prometheus:  ":5090",
		Debug:       "127.0.0.1:5112",
		CPU:         NoCPU,
		Logger:      DefaultLogConfig(),
		Scylla: ScyllaConfig{
			APIAddress:    "0.0.0.0",
			APIPort:       "10000",
			DataDirectory: "/var/lib/scylla/data",
		},
		S3:    rclone.DefaultS3Options(),
		GCS:   rclone.DefaultGCSOptions(),
		Azure: rclone.DefaultAzureOptions(),
	}
}

// ParseAgentConfigFiles takes list of configuration file paths and returns parsed
// config struct with merged configuration from all provided files.
func ParseAgentConfigFiles(files []string) (AgentConfig, error) {
	c := DefaultAgentConfig()
	return c, cfgutil.ParseYAML(&c, files...)
}

func (c AgentConfig) Validate() (errs error) {
	// Validate TLS config
	if c.TLSCertFile == "" {
		errs = multierr.Append(errs, errors.New("missing tls_cert_file"))
	} else if _, err := os.Stat(c.TLSCertFile); err != nil {
		errs = multierr.Append(errs, errors.Wrapf(err, "invalid tls_cert_file %s", c.TLSCertFile))
	}
	if c.TLSKeyFile == "" {
		errs = multierr.Append(errs, errors.New("missing tls_key_file"))
	} else if _, err := os.Stat(c.TLSKeyFile); err != nil {
		errs = multierr.Append(errs, errors.Wrapf(err, "invalid tls_key_file %s", c.TLSKeyFile))
	}
	// Validate Scylla config
	errs = multierr.Append(errs, errors.Wrap(c.Scylla.Validate(), "scylla"))

	// Validate S3 config
	errs = multierr.Append(errs, errors.Wrap(c.S3.Validate(), "s3"))

	return
}

// ObfuscatedAgentConfig returns AgentConfig with secrets replaced with ******.
func ObfuscatedAgentConfig(c AgentConfig) AgentConfig {
	secrets := []*string{
		&c.AuthToken,
		&c.S3.AccessKeyID,
		&c.S3.SecretAccessKey,
		&c.GCS.Token,
		&c.Azure.Key,
	}
	for _, s := range secrets {
		*s = strings.Repeat("*", len(*s))
	}
	return c
}
