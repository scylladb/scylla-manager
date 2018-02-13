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
	AutoScheduleStartTimeMargin time.Duration `yaml:"auto_schedule_start_time_margin"`
	MaxRunAge                   time.Duration `yaml:"max_run_age"`
	SegmentsPerRepair           int           `yaml:"segments_per_repair"`
	SegmentSizeLimit            int           `yaml:"segment_size_limit"`
	SegmentErrorLimit           int           `yaml:"segment_error_limit"`
	PollInterval                time.Duration `yaml:"poll_interval"`
	ErrorBackoff                time.Duration `yaml:"error_backoff"`
}

// DefaultConfig returns a Config initialised with default values.
func DefaultConfig() *Config {
	return &Config{
		AutoScheduleStartTimeMargin: 2 * time.Hour,
		MaxRunAge:                   36 * time.Hour,
		SegmentsPerRepair:           1,
		SegmentSizeLimit:            0,
		SegmentErrorLimit:           100,
		PollInterval:                200 * time.Millisecond,
		ErrorBackoff:                10 * time.Second,
	}
}

// Validate checks if all the fields are properly set.
func (c *Config) Validate() (err error) {
	if c == nil {
		return mermaid.ErrNilPtr
	}

	if c.AutoScheduleStartTimeMargin <= 0 {
		err = multierr.Append(err, errors.New("invalid auto_schedule_start_time_margin, must be > 0"))
	}
	if c.MaxRunAge <= 0 {
		err = multierr.Append(err, errors.New("invalid max_run_age, must be > 0"))
	}
	if c.SegmentsPerRepair <= 0 {
		err = multierr.Append(err, errors.New("invalid segments_per_repair, must be > 0"))
	}
	if c.SegmentSizeLimit < 0 {
		err = multierr.Append(err, errors.New("invalid segment_size_limit, must be > 0 or 0 for no limit"))
	}
	if c.SegmentErrorLimit <= 0 {
		err = multierr.Append(err, errors.New("invalid segment_error_limit, must be > 0"))
	}
	if c.PollInterval <= 0 {
		err = multierr.Append(err, errors.New("invalid poll_interval, must be > 0"))
	}
	if c.ErrorBackoff <= 0 {
		err = multierr.Append(err, errors.New("invalid error_backoff, must be > 0"))
	}

	return
}
