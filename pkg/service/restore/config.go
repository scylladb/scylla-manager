// Copyright (C) 2023 ScyllaDB

package restore

import (
	"github.com/pkg/errors"
	"github.com/scylladb/scylla-manager/v3/pkg/service"
)

// Config specifies the backup service configuration.
type Config struct {
	DiskSpaceFreeMinPercent   int `yaml:"disk_space_free_min_percent"`
	LongPollingTimeoutSeconds int `yaml:"long_polling_timeout_seconds"`
}

func DefaultConfig() Config {
	return Config{
		DiskSpaceFreeMinPercent:   10,
		LongPollingTimeoutSeconds: 10,
	}
}

func (c *Config) Validate() error {
	if c == nil {
		return service.ErrNilPtr
	}

	if c.DiskSpaceFreeMinPercent < 0 || c.DiskSpaceFreeMinPercent >= 100 {
		return errors.New("invalid disk_space_free_min_percent, must be between 0 and 100")
	}
	if c.LongPollingTimeoutSeconds < 0 {
		return errors.New("invalid long_polling_timeout_seconds, must be >= 0")
	}

	return nil
}
