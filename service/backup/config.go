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
	PollInterval            time.Duration `yaml:"poll_interval"`
	AgeMax                  time.Duration `yaml:"age_max"`

	// Additional configuration for testing
	TestS3Endpoint string `yaml:"test_s3_endpoint"`
}

// DefaultConfig returns a Config initialized with default values.
func DefaultConfig() Config {
	return Config{
		DiskSpaceFreeMinPercent: 10,
		PollInterval:            time.Second,
		AgeMax:                  12 * time.Hour,
	}
}

// Validate checks if all the fields are properly set.
func (c *Config) Validate() error {
	var err error
	if c.DiskSpaceFreeMinPercent < 0 || c.DiskSpaceFreeMinPercent >= 100 {
		err = multierr.Append(err, errors.New("invalid disk_space_free_min_percent, must be between 0 and 100"))
	}
	if c.PollInterval <= 0 {
		err = multierr.Append(err, errors.New("invalid poll_interval, must be > 0"))
	}

	return err
}
