// Copyright (C) 2017 ScyllaDB

package agent

import (
	"strings"

	"github.com/pkg/errors"
	"github.com/scylladb/scylla-manager/v3/pkg/config"
	"github.com/scylladb/scylla-manager/v3/pkg/rclone"
	"github.com/scylladb/scylla-manager/v3/pkg/util/cfgutil"
	"go.uber.org/multierr"
)

const (
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

	BroadcastRPCAddress string
	RPCAddress          string
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

// Config specifies the agent and scylla configuration.
type Config struct {
	AuthToken   string               `yaml:"auth_token"`
	HTTPS       string               `yaml:"https"`
	HTTPSPort   int                  `yaml:"https_port"`
	TLSVersion  config.TLSVersion    `yaml:"tls_version"`
	TLSCertFile string               `yaml:"tls_cert_file"`
	TLSKeyFile  string               `yaml:"tls_key_file"`
	Prometheus  string               `yaml:"prometheus"`
	Debug       string               `yaml:"debug"`
	CPU         CPUs                 `yaml:"cpu"`
	Logger      config.LogConfig     `yaml:"logger"`
	Scylla      ScyllaConfig         `yaml:"scylla"`
	Rclone      rclone.GlobalOptions `yaml:"rclone"`
	S3          rclone.S3Options     `yaml:"s3"`
	GCS         rclone.GCSOptions    `yaml:"gcs"`
	Azure       rclone.AzureOptions  `yaml:"azure"`
}

func DefaultConfig() Config {
	return Config{
		HTTPSPort:  10001,
		TLSVersion: config.TLSv12,
		Prometheus: ":5090",
		Debug:      "127.0.0.1:5112",
		CPU:        []int{NoCPU},
		Logger:     DefaultLogConfig(),
		Scylla: ScyllaConfig{
			APIAddress:    "0.0.0.0",
			APIPort:       "10000",
			DataDirectory: "/var/lib/scylla/data",
		},
		Rclone: rclone.DefaultGlobalOptions(),
		S3:     rclone.DefaultS3Options(),
		GCS:    rclone.DefaultGCSOptions(),
		Azure:  rclone.DefaultAzureOptions(),
	}
}

// ParseConfigFiles takes list of configuration file paths and returns parsed
// config struct with merged configuration from all provided files.
func ParseConfigFiles(files []string) (Config, error) {
	c := DefaultConfig()
	err := cfgutil.ParseYAML(&c, DefaultConfig(), files...)

	return c, err
}

func (c Config) Validate() (errs error) {
	// Validate Scylla config
	errs = multierr.Append(errs, errors.Wrap(c.Scylla.Validate(), "scylla"))

	// Validate S3 config
	errs = multierr.Append(errs, errors.Wrap(c.S3.Validate(), "s3"))

	return
}

// HasTLSCert returns true iff TLSCertFile or TLSKeyFile is set.
func (c Config) HasTLSCert() bool {
	return c.TLSCertFile != "" || c.TLSKeyFile != ""
}

// Obfuscate returns Config with secrets replaced with ******.
func Obfuscate(c Config) Config {
	secrets := []*string{
		&c.AuthToken,
		&c.S3.AccessKeyID,
		&c.S3.SecretAccessKey,
		&c.S3.SseCustomerKey,
		&c.GCS.Token,
		&c.GCS.ClientSecret,
		&c.GCS.ServiceAccountCredentials,
		&c.Azure.Key,
	}
	for _, s := range secrets {
		*s = strings.Repeat("*", len(*s))
	}
	return c
}

// CPUs represents a list of CPUs.
// It supports custom marshalling methods which allow
// for specifying 'cpu' in 'scylla-manager-agent.yaml' config
// as both a single int or an array of ints.
type CPUs []int

// UnmarshalYAML is a custom unmarshaller accepting both
// single ints and int array.
func (c *CPUs) UnmarshalYAML(unmarshal func(interface{}) error) error {
	var v int
	errV := unmarshal(&v)
	if errV == nil {
		*c = []int{v}
		return nil
	}
	var arr []int
	errArr := unmarshal(&arr)
	if errArr == nil {
		*c = arr
		return nil
	}
	return errors.Wrap(multierr.Append(errV, errArr), "parse CPU as an int or int array")
}

// MarshalYAML is a custom marshaller returning
// single int is possible, and int array otherwise.
func (c CPUs) MarshalYAML() (interface{}, error) {
	if len(c) == 1 {
		return (c)[0], nil
	}
	return c, nil
}
