// Copyright (C) 2017 ScyllaDB

package cfgutil

import (
	"os"

	"go.uber.org/config"
)

// ParseYAML attempts to load and parse the files given by files and store
// the contents of the files in the struct given by target.
// It will overwrite any conflicting keys by the keys in the subsequent files.
// Missing files will not cause an error but will just be skipped.
func ParseYAML(target interface{}, files ...string) error {
	opts, err := fileOpts(files)
	if err != nil {
		return err
	}
	return parseYaml(target, opts)
}

// PermissiveParseYAML is a variant of ParseYAML that disables gopkg.in/yaml.v2's strict mode
//
// DO NOT USE.
func PermissiveParseYAML(target interface{}, files ...string) error {
	opts, err := fileOpts(files)
	if err != nil {
		return err
	}
	opts = append(opts, config.Permissive())
	return parseYaml(target, opts)
}

func parseYaml(target interface{}, opts []config.YAMLOption) error {
	cfg, err := config.NewYAML(opts...)
	if err != nil {
		return err
	}
	return cfg.Get(config.Root).Populate(target)
}

func fileOpts(files []string) ([]config.YAMLOption, error) {
	var opts []config.YAMLOption
	for _, f := range files {
		exists, err := fileExists(f)
		if err != nil {
			return nil, err
		}
		if exists {
			opts = append(opts, config.File(f))
		}
	}
	return opts, nil
}

func fileExists(filename string) (bool, error) {
	info, err := os.Stat(filename)
	if err != nil {
		if os.IsNotExist(err) {
			return false, nil
		}
		return false, err
	}
	return !info.IsDir(), nil
}
