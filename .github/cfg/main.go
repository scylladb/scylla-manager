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
		cfg.scyllaVersion(),
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

func (cfg integrationTestCfg) scyllaVersion() string {
	return strings.Split(cfg.ScyllaVersion, ":")[1]
}

// This is a simple script used to generate workflow files by applying
// each config from './integration-test-cfg.yaml' onto './integration-test-core.yaml'.
// It also prints github badges syntax that can be pasted into the README.md file.
// It has to be run from the same dir with 'go run main.go <badge-branch>' command.
// The <badge-branch> ('master' by default) arg specifies the branch from which the badges are generated.
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

	// Remove previous integration-test workflows
	entries, err := os.ReadDir("../workflows")
	if err != nil {
		panic(err)
	}
	for _, e := range entries {
		if strings.HasPrefix(e.Name(), "integration-test") {
			if err := os.Remove("../workflows/" + e.Name()); err != nil {
				panic(err)
			}
		}
	}

	// Select badge branch
	branch := "master"
	if len(os.Args) == 2 {
		branch = os.Args[1]
	}

	fmt.Println("Reference links to badges")
	versionBadges := make(map[string][]string)
	for _, cfg := range configs {
		name := cfg.name()
		v := cfg.scyllaVersion()
		core["name"] = name
		core["env"] = cfg
		b, err := yaml.Marshal(&core)
		if err != nil {
			panic(err)
		}
		if err := os.WriteFile("../workflows/"+name+".yaml", b, 0644); err != nil {
			panic(err)
		}

		versionBadges[v] = append(versionBadges[v], "!["+name+"]")
		fmt.Printf("[%s]: https://github.com/scylladb/scylla-manager/actions/workflows/%s.yaml/badge.svg?branch=%s\n",
			name, name, branch)
	}

	fmt.Println("Badges formatted as table")
	fmt.Println("| ScyllaDB version | Workflows | Limitations |")
	fmt.Println("| ---------------- | --------- | ----------- |")
	for v, badges := range versionBadges {
		fmt.Printf("| **%s** | %s | |\n", v, strings.Join(badges, "<br/>"))
	}
}
