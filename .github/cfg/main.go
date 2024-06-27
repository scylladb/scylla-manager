package main

import (
	"fmt"
	"os"
	"strings"

	"gopkg.in/yaml.v3"
)

// integrationTestCfg describes single workflow config from './integration-test-cfg.yaml'.
type integrationTestCfg struct {
	ScyllaVersion string `yaml:"scylla-version"`
	IPFamily      string `yaml:"ip-family"`
	RaftSchema    string `yaml:"raft-schema"`
	Tablets       string `yaml:"tablets"`
}

func (cfg integrationTestCfg) name() string {
	parts := []string{
		"integration",
		"tests",
		strings.Split(cfg.ScyllaVersion, ":")[1],
		cfg.IPFamily,
	}
	if cfg.RaftSchema == "enabled" {
		parts = append(parts, "raftschema")
	}
	if cfg.Tablets == "enabled" {
		parts = append(parts, "tablets")
	}
	return strings.Join(parts, "-")
}

// This is a simple script used to generate workflow files by applying
// each config from './integration-test-cfg.yaml' onto './integration-test-core.yaml'.
// Note that this script does not delete any existing workflows.
// It also prints github badges syntax that can be pasted into the README.md file.
// It has to be run from the same dir with 'go run main.go' command.
func main() {
	f, err := os.ReadFile("./integration-test-cfg.yaml")
	if err != nil {
		panic(err)
	}

	configs := make([]integrationTestCfg, 0)
	if err := yaml.Unmarshal(f, &configs); err != nil {
		panic(err)
	}

	f, err = os.ReadFile("./integration-test-core.yaml")
	if err != nil {
		panic(err)
	}

	core := make(map[any]any)
	if err := yaml.Unmarshal(f, &core); err != nil {
		panic(err)
	}

	for _, cfg := range configs {
		name := cfg.name()
		core["name"] = name
		core["env"] = cfg
		b, err := yaml.Marshal(&core)
		if err != nil {
			panic(err)
		}
		if err := os.WriteFile("../workflows/"+name+".yaml", b, 0644); err != nil {
			panic(err)
		}

		fmt.Printf("![%s](https://github.com/scylladb/scylla-manager/actions/workflows/%s.yaml/badge.svg?branch=master)\n",
			strings.ReplaceAll(name, "-", " "), name)
	}
}
