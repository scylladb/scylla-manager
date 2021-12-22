// Copyright (C) 2017 ScyllaDB

package scheduler

import (
	"time"

	"github.com/gocql/gocql"
	"github.com/pkg/errors"
	"github.com/scylladb/scylla-manager/pkg/scheduler"
	"github.com/scylladb/scylla-manager/pkg/scheduler/trigger"
)

// Cron implements a trigger based on cron expression.
// It supports the extended syntax including @monthly, @weekly, @daily, @midnight, @hourly, @every <time.Duration>.
type Cron struct {
	spec  []byte
	inner scheduler.Trigger
}

func NewCron(spec string) (Cron, error) {
	t, err := trigger.NewCron(spec)
	if err != nil {
		return Cron{}, err
	}

	return Cron{
		spec:  []byte(spec),
		inner: t,
	}, nil
}

func NewCronEvery(d time.Duration) Cron {
	c, _ := NewCron("@every " + d.String()) // nolint: errcheck
	return c
}

// MustCron calls NewCron and panics on error.
func MustCron(spec string) Cron {
	c, err := NewCron(spec)
	if err != nil {
		panic(err)
	}
	return c
}

// Next implements scheduler.Trigger.
func (c Cron) Next(now time.Time) time.Time {
	if c.inner == nil {
		return time.Time{}
	}
	return c.inner.Next(now)
}

func (c Cron) MarshalText() (text []byte, err error) {
	return c.spec, nil
}

func (c *Cron) UnmarshalText(text []byte) error {
	if len(text) == 0 {
		return nil
	}

	v, err := NewCron(string(text))
	if err != nil {
		return errors.Wrap(err, "cron")
	}

	*c = v
	return nil
}

func (c Cron) MarshalCQL(info gocql.TypeInfo) ([]byte, error) {
	return c.MarshalText()
}

func (c *Cron) UnmarshalCQL(info gocql.TypeInfo, data []byte) error {
	return c.UnmarshalText(data)
}

func (c Cron) IsZero() bool {
	return c.inner == nil
}
