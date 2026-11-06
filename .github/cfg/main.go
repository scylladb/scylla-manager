package main

import (
	"fmt"
	"os"
	"strings"

	"gopkg.in/yaml.v3"
)

const (
	cfgFile      = "./integration-test-cfg.yaml"
	callerFile   = "./integration-test-caller.yaml"
	workflowsDir = "../workflows"
	// Prefix of all generated workflow files.
	// Note that it does not match the reusable 'integration-tests.yaml' workflow.
	generatedPrefix = "integration-tests-"
	// Name of the generated workflow running all release suite configs at once.
	releaseName = "integration-tests-release"

	suitePR      = "pr"
	suiteRelease = "release"
)

// integrationTestCfg describes single workflow config from './integration-test-cfg.yaml'.
type integrationTestCfg struct {
	// Suite decides when the workflow is triggered:
	//  - pr: on every PR and push to master or release branch, and manually
	//  - release: weekly and manually
	Suite  string                `yaml:"suite"`
	Inputs integrationTestInputs `yaml:",inline"`
}

// integrationTestInputs are passed as inputs to the reusable 'integration-tests.yaml' workflow.
type integrationTestInputs struct {
	ScyllaVersion  string `yaml:"scylla-version"`
	IPFamily       string `yaml:"ip-family"`
	Tablets        string `yaml:"tablets"`
	SSLEnabled     string `yaml:"ssl-enabled,omitempty"`
	BackupMethod   string `yaml:"backup-method,omitempty"`
	RestoreMethod  string `yaml:"restore-method,omitempty"`
	BackupProvider string `yaml:"backup-provider,omitempty"`
}

// callerInputs are passed as 'with' to the reusable 'integration-tests.yaml' workflow.
type callerInputs struct {
	// ConfigName is used by the reusable workflow to tell apart artifacts
	// of different configs run in a single 'integration-tests-release' run.
	ConfigName            string `yaml:"config-name"`
	integrationTestInputs `yaml:",inline"`
}

func (cfg integrationTestCfg) name() string {
	in := cfg.Inputs
	parts := []string{
		"integration",
		"tests",
		cfg.scyllaVersion(),
		in.IPFamily,
	}
	if in.Tablets == "enabled" {
		parts = append(parts, "tablets")
	}
	if in.SSLEnabled == "false" {
		parts = append(parts, "nossl")
	}
	if in.BackupMethod != "" {
		parts = append(parts, in.BackupMethod)
	}
	if in.RestoreMethod != "" {
		parts = append(parts, in.RestoreMethod)
	}
	if in.BackupProvider != "" {
		parts = append(parts, in.BackupProvider)
	}
	return strings.Join(parts, "-")
}

func (cfg integrationTestCfg) scyllaVersion() string {
	return strings.Split(cfg.Inputs.ScyllaVersion, ":")[1]
}

// This is a simple script used to generate workflow files by applying
// each config from './integration-test-cfg.yaml' onto './integration-test-caller.yaml'.
// It also generates 'integration-tests-release.yaml' workflow running all release suite
// configs at once and prints github badges syntax that can be pasted into the README.md file.
// It has to be run from the same dir with 'go run main.go' command.
func main() {
	f, err := os.ReadFile(cfgFile)
	if err != nil {
		panic(err)
	}
	configs := make([]integrationTestCfg, 0)
	if err := yaml.Unmarshal(f, &configs); err != nil {
		panic(err)
	}
	for _, cfg := range configs {
		if cfg.Suite != suitePR && cfg.Suite != suiteRelease {
			panic(fmt.Sprintf("config %s: unknown suite %q", cfg.name(), cfg.Suite))
		}
	}

	callerTemplate, err := os.ReadFile(callerFile)
	if err != nil {
		panic(err)
	}

	// Remove previously generated workflows
	entries, err := os.ReadDir(workflowsDir)
	if err != nil {
		panic(err)
	}
	for _, e := range entries {
		if strings.HasPrefix(e.Name(), generatedPrefix) {
			if err := os.Remove(workflowsDir + "/" + e.Name()); err != nil {
				panic(err)
			}
		}
	}

	releaseJobs := make(map[string]any)
	for _, cfg := range configs {
		name := cfg.name()
		writeWorkflow(name, callerWorkflow(cfg, callerTemplate))
		if cfg.Suite != suiteRelease {
			continue
		}
		// Job ID can't contain dots
		releaseJobs[strings.ReplaceAll(name, ".", "-")] = map[string]any{
			"name": name,
			"uses": "./.github/workflows/" + name + ".yaml",
		}
	}
	writeWorkflow(releaseName, map[string]any{
		"name": releaseName,
		"on": map[string]any{
			"workflow_dispatch": map[string]any{},
		},
		"permissions": map[string]any{
			"contents": "read",
		},
		"jobs": releaseJobs,
	})

	printBadges(configs)
}

// callerWorkflow applies cfg onto the caller template.
func callerWorkflow(cfg integrationTestCfg, callerTemplate []byte) map[string]any {
	// Template is unmarshalled per config, as it is modified in place
	wf := make(map[string]any)
	if err := yaml.Unmarshal(callerTemplate, &wf); err != nil {
		panic(err)
	}
	name := cfg.name()

	wf["name"] = name
	wf["concurrency"].(map[string]any)["group"] = "int-" + name + "-${{ github.ref }}"
	on := wf["on"].(map[string]any)
	switch cfg.Suite {
	case suitePR:
		delete(on, "schedule")
		delete(on, "workflow_call")
	case suiteRelease:
		delete(on, "push")
		delete(on, "pull_request")
	}
	wf["jobs"].(map[string]any)["tests"].(map[string]any)["with"] = callerInputs{
		ConfigName:            name,
		integrationTestInputs: cfg.Inputs,
	}
	return wf
}

func writeWorkflow(name string, wf map[string]any) {
	b, err := yaml.Marshal(wf)
	if err != nil {
		panic(err)
	}
	if err := os.WriteFile(workflowsDir+"/"+name+".yaml", b, 0o644); err != nil {
		panic(err)
	}
}

func printBadges(configs []integrationTestCfg) {
	fmt.Println("Reference links to badges")
	for _, cfg := range configs {
		name := cfg.name()
		fmt.Printf("[%s]: https://github.com/scylladb/scylla-manager/actions/workflows/%s.yaml/badge.svg?branch=master\n",
			name, name)
	}

	fmt.Println("Badges formatted as tables")
	for _, suite := range []string{suitePR, suiteRelease} {
		var versions []string
		versionBadges := make(map[string][]string)
		for _, cfg := range configs {
			if cfg.Suite != suite {
				continue
			}
			v := cfg.scyllaVersion()
			if _, ok := versionBadges[v]; !ok {
				versions = append(versions, v)
			}
			versionBadges[v] = append(versionBadges[v], "!["+cfg.name()+"]")
		}

		fmt.Printf("\n%s suite\n", suite)
		fmt.Println("| ScyllaDB version | Workflows | Limitations |")
		fmt.Println("| ---------------- | --------- | ----------- |")
		for _, v := range versions {
			fmt.Printf("| **%s** | %s | |\n", v, strings.Join(versionBadges[v], "<br/>"))
		}
	}
}
