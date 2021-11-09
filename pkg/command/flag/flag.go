// Copyright (C) 2017 ScyllaDB

package flag

import (
	"os"

	"github.com/scylladb/go-set/strset"
	"github.com/scylladb/scylla-manager/pkg/config"
	"github.com/spf13/cobra"
	flag "github.com/spf13/pflag"
	"gopkg.in/yaml.v2"
)

// Wrapper add support for common flags.
type Wrapper struct {
	fs *flag.FlagSet
}

// Wrap returns a flag set wrapper.
func Wrap(fs *flag.FlagSet) Wrapper {
	return Wrapper{fs: fs}
}

func (w Wrapper) Unwrap() *flag.FlagSet {
	return w.fs
}

var keywords = strset.New(
	"use",
	"deprecated",
	"example",
	"short",
	"long",
)

// MustSetUsages takes a yaml encoded mapping from flag name to usage.
// It ensures that all flags have usage and the mapping does not contain
// unrelated items.
func MustSetUsages(cmd *cobra.Command, b []byte, required ...string) {
	u := make(map[string]string)
	if err := yaml.Unmarshal(b, u); err != nil {
		panic(err)
	}

	fs := cmd.Flags()
	// Set usages from file
	for k, v := range u {
		if keywords.Has(k) {
			continue
		}
		f := fs.Lookup(k)
		if f == nil {
			panic("missing flag " + k)
		}
		f.Usage = cleanup(v)
	}
	// Make sure flags are set
	fs.Visit(func(f *flag.Flag) {
		if f.Usage == "" {
			panic("no usage for flag " + f.Name)
		}
	})
	// Mark flags as required
	for _, name := range required {
		if err := cmd.MarkFlagRequired(name); err != nil {
			panic(err)
		}
	}
}

//
// Task schedule flags
//

func (w Wrapper) enabled(p *bool) {
	w.fs.BoolVar(p, "enabled", true, usage["enabled"])
}

func (w Wrapper) interval(p *Duration) {
	w.fs.VarP(p, "interval", "i", usage["interval"])
}

func (w Wrapper) startDate(p *Time) {
	w.fs.VarP(p, "start-date", "s", usage["start-date"])
}

func (w Wrapper) numRetries(p *int, def int) {
	w.fs.IntVarP(p, "num-retries", "r", def, usage["num-retries"])
}

//
// Common flags
//

func (w Wrapper) Cluster(p *string) {
	w.fs.StringVarP(p, "cluster", "c", os.Getenv("SCYLLA_MANAGER_CLUSTER"), usage["cluster"])
}

func (w Wrapper) Datacenter(p *[]string) {
	w.fs.StringSliceVar(p, "dc", nil, usage["dc"])
}

func (w Wrapper) FailFast(p *bool) {
	w.fs.BoolVar(p, "fail-fast", false, usage["fail-fats"])
}

func (w Wrapper) Keyspace(p *[]string) {
	w.fs.StringSliceVarP(p, "keyspace", "K", nil, usage["keyspace"])
}

func (w Wrapper) Location(p *[]string) {
	w.fs.StringSliceVarP(p, "location", "L", nil, usage["location"])
}

//
// Global flags
//

func (w Wrapper) GlobalAPIURL(p *string) {
	w.fs.StringVar(p, "api-url", apiURL(), usage["api-url"])
}

func (w Wrapper) GlobalAPICertFile(p *string) {
	w.fs.StringVar(p, "api-cert-file", os.Getenv("SCYLLA_MANAGER_API_CERT_FILE"), usage["api-cert-file"])
}

func (w Wrapper) GlobalAPIKeyFile(p *string) {
	w.fs.StringVar(p, "api-key-file", os.Getenv("SCYLLA_MANAGER_API_KEY_FILE"), usage["api-key-file"])
}

func apiURL() string {
	if v := os.Getenv("SCYLLA_MANAGER_API_URL"); v != "" {
		return v
	}
	if cfg, err := config.ParseServerConfigFiles([]string{"/etc/scylla-manager/scylla-manager.yaml"}); err == nil {
		if v := cfg.BaseURL(); v != "" {
			return v
		}
	}
	return "http://127.0.0.1:5080/api/v1"
}
