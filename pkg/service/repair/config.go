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
	SegmentsPerRepair      int           `yaml:"segments_per_repair"`
	ShardParallelMax       int           `yaml:"shard_parallel_max"`
	ShardFailedSegmentsMax int           `yaml:"shard_failed_segments_max"`
	PollInterval           time.Duration `yaml:"poll_interval"`
	ErrorBackoff           time.Duration `yaml:"error_backoff"`
	AgeMax                 time.Duration `yaml:"age_max"`
	ShardingIgnoreMsbBits  int           `yaml:"murmur3_partitioner_ignore_msb_bits"`
}

// DefaultConfig returns a Config initialized with default values.
func DefaultConfig() Config {
	return Config{
		SegmentsPerRepair:      1,
		ShardFailedSegmentsMax: 100,
		ErrorBackoff:           5 * time.Minute,
		PollInterval:           200 * time.Millisecond,
		AgeMax:                 36 * time.Hour,
		ShardingIgnoreMsbBits:  12,
	}
}

// Validate checks if all the fields are properly set.
func (c *Config) Validate() error {
	if c == nil {
		return service.ErrNilPtr
	}

	var err error
	if c.SegmentsPerRepair <= 0 {
		err = multierr.Append(err, errors.New("invalid segments_per_repair, must be > 0"))
	}
	if c.ShardParallelMax < 0 {
		err = multierr.Append(err, errors.New("invalid shard_parallel_max, must be > 0 or 0 for no limit"))
	}
	if c.ShardFailedSegmentsMax < 0 {
		err = multierr.Append(err, errors.New("invalid shard_failed_segments_max, must be >= 0"))
	}
	if c.ErrorBackoff <= 0 {
		err = multierr.Append(err, errors.New("invalid error_backoff, must be > 0"))
	}
	if c.PollInterval <= 0 {
		err = multierr.Append(err, errors.New("invalid poll_interval, must be > 0"))
	}
	if c.AgeMax <= 0 {
		err = multierr.Append(err, errors.New("invalid age_max, must be > 0"))
	}
	if c.ShardingIgnoreMsbBits < 0 {
		err = multierr.Append(err, errors.New("invalid murmur3_partitioner_ignore_msb_bits, must be >= 0"))
	}

	return err
}
