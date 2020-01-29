// Copyright (C) 2017 ScyllaDB

package backup

import (
	"time"

	"github.com/pkg/errors"
	"go.uber.org/multierr"
)

// Config specifies the backup service configuration.
type Config struct {
	DiskSpaceFreeMinPercent int           `yaml:"disk_space_free_min_percent"`
	AgeMax                  time.Duration `yaml:"age_max"`
}

// DefaultConfig returns a Config initialized with default values.
func DefaultConfig() Config {
	return Config{
		DiskSpaceFreeMinPercent: 10,
		AgeMax:                  12 * time.Hour,
	}
}

// Validate checks if all the fields are properly set.
func (c *Config) Validate() error {
	var err error
	if c.DiskSpaceFreeMinPercent < 0 || c.DiskSpaceFreeMinPercent >= 100 {
		err = multierr.Append(err, errors.New("invalid disk_space_free_min_percent, must be between 0 and 100"))
	}

	return err
}
