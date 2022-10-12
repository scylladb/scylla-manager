// Copyright (C) 2017 ScyllaDB

package backup

import (
	"time"

	"github.com/pkg/errors"
	"go.uber.org/multierr"

	"github.com/scylladb/scylla-manager/v3/pkg/service"
)

// Config specifies the backup service configuration.
type Config struct {
	DiskSpaceFreeMinPercent   int           `yaml:"disk_space_free_min_percent"`
	LongPollingTimeoutSeconds int           `yaml:"long_polling_timeout_seconds"`
	AgeMax                    time.Duration `yaml:"age_max"`
	MaxLoadThreshold          float64       `yaml:"max_load_threshold"`
}

func DefaultConfig() Config {
	return Config{
		DiskSpaceFreeMinPercent:   10,
		LongPollingTimeoutSeconds: 10,
		AgeMax:                    12 * time.Hour,
		MaxLoadThreshold:          100,
	}
}

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
	if c.MaxLoadThreshold < 0 {
		err = multierr.Append(err, errors.New("invalid max_load_threshold, must be >= 0"))
	}

	return err
}
