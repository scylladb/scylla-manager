// Copyright (C) 2017 ScyllaDB

package repair

import (
	"time"

	"github.com/pkg/errors"
	"github.com/scylladb/mermaid/pkg/service"
	"go.uber.org/multierr"
)

// Config specifies the repair service configuration.
type Config struct {
	PollInterval                    time.Duration `yaml:"poll_interval"`
	AgeMax                          time.Duration `yaml:"age_max"`
	GracefulShutdownTimeout         time.Duration `yaml:"graceful_shutdown_timeout"`
	ForceRowLevelRepair             bool          `yaml:"force_row_level_repair"`
	ForceLegacyRepair               bool          `yaml:"force_legacy_repair"`
	Murmur3PartitionerIgnoreMSBBits int           `yaml:"murmur3_partitioner_ignore_msb_bits"`
}

// DefaultConfig returns a Config initialized with default values.
func DefaultConfig() Config {
	return Config{
		PollInterval:                    50 * time.Millisecond,
		GracefulShutdownTimeout:         30 * time.Second,
		Murmur3PartitionerIgnoreMSBBits: 12,
	}
}

// Validate checks if all the fields are properly set.
func (c *Config) Validate() error {
	if c == nil {
		return service.ErrNilPtr
	}

	var err error
	if c.PollInterval <= 0 {
		err = multierr.Append(err, errors.New("invalid poll_interval, must be > 0"))
	}
	if c.AgeMax < 0 {
		err = multierr.Append(err, errors.New("invalid age_max, must be >= 0"))
	}
	if c.GracefulShutdownTimeout <= 0 {
		err = multierr.Append(err, errors.New("invalid graceful_shutdown_timeout, must be > 0"))
	}
	if c.Murmur3PartitionerIgnoreMSBBits < 0 {
		err = multierr.Append(err, errors.New("invalid murmur3_partitioner_ignore_msb_bits, must be >= 0"))
	}

	return err
}
