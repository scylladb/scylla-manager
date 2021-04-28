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
	var opts []config.YAMLOption
	for _, f := range files {
		exists, err := fileExists(f)
		if err != nil {
			return err
		}
		if exists {
			opts = append(opts, config.File(f))
		}
	}
	cfg, err := config.NewYAML(opts...)
	if err != nil {
		return err
	}
	return cfg.Get(config.Root).Populate(target)
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
