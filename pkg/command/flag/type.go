// Copyright (C) 2017 ScyllaDB

package flag

import (
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/pkg/errors"
	"github.com/scylladb/scylla-manager/pkg/util/duration"
	"github.com/scylladb/scylla-manager/pkg/util/timeutc"
	flag "github.com/spf13/pflag"
)

// Time wraps time.Time and add support for now+duration syntax.
type Time struct {
	v time.Time
}

var _ flag.Value = (*Time)(nil)

func (t *Time) String() string {
	if t.v.IsZero() {
		return ""
	}
	return t.v.String()
}

// Set implements pflag.Value.
func (t *Time) Set(s string) (err error) {
	t.v, err = parserTime(s)
	return
}

func (t *Time) Value() time.Time {
	if t.v.IsZero() {
		return timeutc.Now()
	}
	return t.v
}

func parserTime(s string) (time.Time, error) {
	if strings.HasPrefix(s, "now") {
		now := timeutc.Now()
		if s == "now" {
			return now, nil
		}
		d, err := duration.ParseDuration(s[3:])
		if err != nil {
			return time.Time{}, err
		}
		return now.Add(d.Duration()), nil
	}

	t, err := timeutc.Parse(time.RFC3339, s)
	if err != nil {
		return time.Time{}, err
	}
	return t.UTC(), nil
}

// Type implements pflag.Value.
func (t *Time) Type() string {
	return "string"
}

// Duration wraps duration.Duration that adds support for days to time.Duration.
type Duration struct {
	v duration.Duration
}

var _ flag.Value = (*Duration)(nil)

func (d *Duration) String() string {
	if d.v == 0 {
		return ""
	}
	return d.v.String()
}

// Set implements pflag.Value.
func (d *Duration) Set(s string) (err error) {
	d.v, err = duration.ParseDuration(s)
	return
}

// Type implements pflag.Value.
func (d *Duration) Type() string {
	return "string"
}

func (d *Duration) Value() duration.Duration {
	return d.v
}

// Intensity represents intensity flag which is a float64 value with a custom validation.
type Intensity struct {
	v float64
}

func NewIntensity(v float64) *Intensity {
	return &Intensity{v: v}
}

func (fl *Intensity) String() string {
	return fmt.Sprint(fl.v)
}

// Set implements pflag.Value.
func (fl *Intensity) Set(s string) error {
	errValidation := errors.New("intensity must be an integer >= 1 or a decimal between (0,1)")

	f, err := strconv.ParseFloat(s, 64)
	if err != nil {
		return errValidation
	}
	if f > 1 {
		_, err := strconv.ParseInt(s, 10, 64)
		if err != nil {
			return errValidation
		}
	}

	fl.v = f
	return nil
}

// Type implements pflag.Value.
func (fl *Intensity) Type() string {
	return "float64"
}

func (fl *Intensity) Value() float64 {
	return fl.v
}
