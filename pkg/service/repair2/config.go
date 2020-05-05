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
	ErrorBackoff          time.Duration `yaml:"error_backoff"`
	PollInterval          time.Duration `yaml:"poll_interval"`
	AgeMax                time.Duration `yaml:"age_max"`
	ShardingIgnoreMsbBits int           `yaml:"murmur3_partitioner_ignore_msb_bits"`
}

// DefaultConfig returns a Config initialized with default values.
func DefaultConfig() Config {
	return Config{
		ErrorBackoff:          5 * time.Minute,
		PollInterval:          200 * time.Millisecond,
		ShardingIgnoreMsbBits: 12,
	}
}

// Validate checks if all the fields are properly set.
func (c *Config) Validate() error {
	if c == nil {
		return service.ErrNilPtr
	}

	var err error
	if c.ErrorBackoff <= 0 {
		err = multierr.Append(err, errors.New("invalid error_backoff, must be > 0"))
	}
	if c.PollInterval <= 0 {
		err = multierr.Append(err, errors.New("invalid poll_interval, must be > 0"))
	}
	if c.AgeMax < 0 {
		err = multierr.Append(err, errors.New("invalid age_max, must be >= 0"))
	}
	if c.ShardingIgnoreMsbBits < 0 {
		err = multierr.Append(err, errors.New("invalid murmur3_partitioner_ignore_msb_bits, must be >= 0"))
	}

	return err
}
