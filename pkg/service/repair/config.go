// Copyright (C) 2017 ScyllaDB

package repair

import (
	"time"

	"github.com/pkg/errors"
	"github.com/scylladb/scylla-manager/pkg/service"
	"go.uber.org/multierr"
)

// Type represents type of the repair algorithm.
type Type string

const (
	// TypeAuto auto detects repair algo.
	TypeAuto Type = "auto"
	// TypeRowLevel row level repair.
	TypeRowLevel Type = "row_level"
	// TypeLegacy legacy repair type.
	TypeLegacy Type = "legacy"
)

// Config specifies the repair service configuration.
type Config struct {
	PollInterval                    time.Duration `yaml:"poll_interval"`
	LongPollingTimeoutSeconds       int           `yaml:"long_polling_timeout_seconds"`
	AgeMax                          time.Duration `yaml:"age_max"`
	GracefulStopTimeout             time.Duration `yaml:"graceful_stop_timeout"`
	ForceRepairType                 Type          `yaml:"force_repair_type"`
	Murmur3PartitionerIgnoreMSBBits int           `yaml:"murmur3_partitioner_ignore_msb_bits"`
}

func DefaultConfig() Config {
	return Config{
		PollInterval:                    50 * time.Millisecond,
		LongPollingTimeoutSeconds:       10,
		GracefulStopTimeout:             30 * time.Second,
		ForceRepairType:                 TypeAuto,
		Murmur3PartitionerIgnoreMSBBits: 12,
	}
}

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
	if c.GracefulStopTimeout <= 0 {
		err = multierr.Append(err, errors.New("invalid graceful_stop_timeout, must be > 0"))
	}
	switch c.ForceRepairType {
	case TypeAuto, TypeRowLevel, TypeLegacy:
	default:
		err = multierr.Append(err, errors.Errorf("invalid force_repair_type value %s", c.ForceRepairType))
	}
	if c.Murmur3PartitionerIgnoreMSBBits < 0 {
		err = multierr.Append(err, errors.New("invalid murmur3_partitioner_ignore_msb_bits, must be >= 0"))
	}

	return err
}
