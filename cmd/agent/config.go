// Copyright (C) 2017 ScyllaDB

package main

import (
	"io/ioutil"
	"os"

	"github.com/rclone/rclone/fs"

	"github.com/pkg/errors"
	"go.uber.org/multierr"
	"gopkg.in/yaml.v2"
)

const defaultHTTPSPort = "10001"

type logConfig struct {
	Level fs.LogLevel `yaml:"level"`
}

// scyllaConfig contains selected elements of Scylla configuration.
type scyllaConfig struct {
	ListenAddress     string `yaml:"listen_address"`
	BroadcastAddress  string `yaml:"broadcast_address"`
	APIAddress        string `yaml:"api_address"`
	APIPort           string `yaml:"api_port"`
	PrometheusAddress string `yaml:"prometheus_address"`
	PrometheusPort    string `yaml:"prometheus_port"`
}

// config specifies the agent and scylla configuration.
type config struct {
	HTTPS            string       `yaml:"https"`
	TLSCertFile      string       `yaml:"tls_cert_file"`
	TLSKeyFile       string       `yaml:"tls_key_file"`
	CPU              int          `yaml:"cpu"`
	ScyllaConfigFile string       `yaml:"scylla_config_file"`
	Scylla           scyllaConfig `yaml:"scylla"`
	Logger           logConfig    `yaml:"logger"`
}

func defaultConfig() config {
	return config{
		TLSCertFile:      "/var/lib/scylla-manager/scylla_manager.crt",
		TLSKeyFile:       "/var/lib/scylla-manager/scylla_manager.key",
		CPU:              -1,
		ScyllaConfigFile: "/etc/scylla/scylla.yaml",
		Scylla: scyllaConfig{
			APIAddress:        "127.0.0.1",
			APIPort:           "10000",
			PrometheusAddress: "0.0.0.0",
			PrometheusPort:    "9180",
		},
		Logger: logConfig{
			fs.LogLevelInfo,
		},
	}
}

func (c *config) UnmarshalYAML(unmarshal func(interface{}) error) error {
	*c = defaultConfig()

	// Update Scylla config file if changed
	aux := struct {
		ScyllaConfigFile string `yaml:"scylla_config_file"`
	}{}
	if err := unmarshal(&aux); err != nil {
		return err
	}
	if aux.ScyllaConfigFile != "" {
		c.ScyllaConfigFile = aux.ScyllaConfigFile
	}

	// Read Scylla config file and update Scylla defaults
	d, err := ioutil.ReadFile(c.ScyllaConfigFile)
	if err != nil {
		return errors.Wrapf(err, "invalid scylla_config_file %s", aux.ScyllaConfigFile)
	}
	if err := yaml.Unmarshal(d, &c.Scylla); err != nil {
		return errors.Wrapf(err, "invalid scylla_config_file %s", aux.ScyllaConfigFile)
	}

	type plain config
	if err := unmarshal((*plain)(c)); err != nil {
		return err
	}

	// Set HTTPS based on Scylla broadcast address if not set
	if c.HTTPS == "" {
		addr, err := httpsListenAddr(c.Scylla)
		if err != nil {
			return err
		}
		c.HTTPS = addr + ":" + defaultHTTPSPort
	}

	return c.validate()
}

func (c config) validate() (errs error) {
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
	errs = multierr.Append(errs, errors.Wrap(c.Scylla.validate(), "scylla"))

	return
}

func (c scyllaConfig) validate() (errs error) {
	if c.ListenAddress == "" && c.BroadcastAddress == "" {
		errs = multierr.Append(errs, errors.New("missing listen_address and broadcast_address"))
	}
	if c.APIAddress == "" {
		errs = multierr.Append(errs, errors.New("missing api_address"))
	}
	if c.APIPort == "" {
		errs = multierr.Append(errs, errors.New("missing api_port"))
	}
	if c.PrometheusAddress == "" {
		errs = multierr.Append(errs, errors.New("missing prometheus_address"))
	}
	if c.PrometheusPort == "" {
		errs = multierr.Append(errs, errors.New("missing prometheus_port"))
	}
	return
}
