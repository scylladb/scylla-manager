// Copyright (C) 2017 ScyllaDB

package backup

import (
	"time"

	"github.com/pkg/errors"
	"github.com/scylladb/scylla-manager/pkg/service"
	"go.uber.org/multierr"
)

// Config specifies the backup service configuration.
type Config struct {
	DiskSpaceFreeMinPercent   int           `yaml:"disk_space_free_min_percent"`
	LongPollingTimeoutSeconds int           `yaml:"long_polling_timeout_seconds"`
	AgeMax                    time.Duration `yaml:"age_max"`
}

// DefaultConfig returns a Config initialized with default values.
func DefaultConfig() Config {
	return Config{
		DiskSpaceFreeMinPercent:   10,
		LongPollingTimeoutSeconds: 10,
		AgeMax:                    12 * time.Hour,
	}
}

// Validate checks if all the fields are properly set.
func (c *Config) Validate() error {
	if c == nil {
		return service.ErrNilPtr
	}

	var err error
	if c.DiskSpaceFreeMinPercent < 0 || c.DiskSpaceFreeMinPercent >= 100 {
		err = multierr.Append(err, errors.New("invalid disk_space_free_min_percent, must be between 0 and 100"))
	}
	if c.AgeMax < 0 {
		err = multierr.Append(err, errors.New("invalid age_max, must be >= 0"))
	}

	return err
}
