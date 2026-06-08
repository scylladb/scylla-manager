// Copyright (C) 2026 ScyllaDB

package backup

import (
	"time"

	"github.com/pkg/errors"
	"github.com/scylladb/scylla-manager/v3/pkg/util"
	"go.uber.org/multierr"
)

// Config specifies the backup service configuration.
type Config struct {
	DiskSpaceFreeMinPercent   int           `yaml:"disk_space_free_min_percent"`
	LongPollingTimeoutSeconds int           `yaml:"long_polling_timeout_seconds"`
	AgeMax                    time.Duration `yaml:"age_max"`
	// PermissionCheckFilesTTL defines the time after which leftover
	// permission check files are considered stale and can be deleted.
	// Setting it to zero disables the cleanup.
	PermissionCheckFilesTTL time.Duration `yaml:"permission_check_files_ttl"`
}

func DefaultConfig() Config {
	return Config{
		DiskSpaceFreeMinPercent:   10,
		LongPollingTimeoutSeconds: 10,
		AgeMax:                    24 * time.Hour,
		PermissionCheckFilesTTL:   5 * time.Minute,
	}
}

func (c *Config) Validate() error {
	if c == nil {
		return util.ErrNilPtr
	}

	var err error
	if c.DiskSpaceFreeMinPercent < 0 || c.DiskSpaceFreeMinPercent >= 100 {
		err = multierr.Append(err, errors.New("invalid disk_space_free_min_percent, must be between 0 and 100"))
	}
	if c.AgeMax < 0 {
		err = multierr.Append(err, errors.New("invalid age_max, must be >= 0"))
	}
	if c.PermissionCheckFilesTTL < 0 {
		err = multierr.Append(err, errors.New("invalid permission_check_files_ttl, must be >= 0"))
	}

	return err
}
