// Copyright (C) 2017 ScyllaDB

package main

import (
	"io/ioutil"

	"github.com/pkg/errors"
	"gopkg.in/yaml.v2"
)

const (
	defaultConfigFile       = "/etc/scylla-manager-agent/scylla-manager-agent.yaml"
	defaultScyllaConfigFile = "/etc/scylla/scylla.yaml"
)

// config specifies the agent configuration.
type config struct {
	HTTP   string       `yaml:"http"`
	Scylla scyllaConfig `yaml:"scylla"`
}

// scyllaConfig represents Scylla configuration.
type scyllaConfig struct {
	APIPort        string `yaml:"api_port"`
	PrometheusPort string `yaml:"prometheus_port"`
}

// defaultConfig returns a config initialized with default values.
func defaultConfig() config {
	return config{
		HTTP: "127.0.0.1:10001",
		Scylla: scyllaConfig{
			APIPort:        "10000",
			PrometheusPort: "9180",
		},
	}
}

// parseConfigFiles parses the agent and Scylla server configuration files.
func parseConfigFiles(configFile, scyllaConfigFile string) (config, error) {
	c := defaultConfig()
	if scyllaConfigFile != "" {
		if err := unmarshalFile(scyllaConfigFile, &c.Scylla); err != nil {
			return config{}, errors.Wrapf(err, "failed to parse %q", scyllaConfigFile)
		}
	}
	if err := unmarshalFile(configFile, &c); err != nil {
		return config{}, errors.Wrapf(err, "failed to parse %q", configFile)
	}
	return c, nil
}

func unmarshalFile(filename string, v interface{}) error {
	b, err := ioutil.ReadFile(filename)
	if err != nil {
		return err
	}
	if err := yaml.Unmarshal(b, v); err != nil {
		return err
	}
	return nil
}
