// Copyright (C) 2017 ScyllaDB

package repair

import (
	"time"

	"github.com/pkg/errors"
	"github.com/scylladb/mermaid"
	"go.uber.org/multierr"
)

// Config specifies the repair service configuration.
type Config struct {
	SegmentSizeLimit      int           `yaml:"segment_size_limit"`
	SegmentsPerRepair     int           `yaml:"segments_per_repair"`
	SegmentErrorLimit     int           `yaml:"segment_error_limit"`
	PollInterval          time.Duration `yaml:"poll_interval"`
	ErrorBackoff          time.Duration `yaml:"error_backoff"`
	MaxRunAge             time.Duration `yaml:"max_run_age"`
	ShardingIgnoreMsbBits int           `yaml:"murmur3_partitioner_ignore_msb_bits"`
}

// DefaultConfig returns a Config initialised with default values.
func DefaultConfig() Config {
	return Config{
		SegmentSizeLimit:      0,
		SegmentsPerRepair:     1,
		SegmentErrorLimit:     100,
		ErrorBackoff:          10 * time.Second,
		PollInterval:          200 * time.Millisecond,
		MaxRunAge:             36 * time.Hour,
		ShardingIgnoreMsbBits: 12,
	}
}

// Validate checks if all the fields are properly set.
func (c *Config) Validate() error {
	if c == nil {
		return mermaid.ErrNilPtr
	}

	var err error
	if c.SegmentSizeLimit < 0 {
		err = multierr.Append(err, errors.New("invalid segment_size_limit, must be > 0 or 0 for no limit"))
	}
	if c.SegmentsPerRepair <= 0 {
		err = multierr.Append(err, errors.New("invalid segments_per_repair, must be > 0"))
	}
	if c.SegmentErrorLimit <= 0 {
		err = multierr.Append(err, errors.New("invalid segment_error_limit, must be > 0"))
	}
	if c.ErrorBackoff <= 0 {
		err = multierr.Append(err, errors.New("invalid error_backoff, must be > 0"))
	}
	if c.PollInterval <= 0 {
		err = multierr.Append(err, errors.New("invalid poll_interval, must be > 0"))
	}
	if c.MaxRunAge <= 0 {
		err = multierr.Append(err, errors.New("invalid max_run_age, must be > 0"))
	}
	if c.ShardingIgnoreMsbBits < 0 {
		err = multierr.Append(err, errors.New("invalid murmur3_partitioner_ignore_msb_bits, must be >= 0"))
	}

	return err
}
