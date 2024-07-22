// Copyright (C) 2017 ScyllaDB

package flag

import (
	"encoding/csv"
	"fmt"
	"maps"
	"strconv"
	"strings"
	"time"

	"github.com/go-openapi/strfmt"
	"github.com/pkg/errors"
	"github.com/robfig/cron/v3"
	"github.com/scylladb/scylla-manager/v3/pkg/util/duration"
	"github.com/scylladb/scylla-manager/v3/pkg/util/timeutc"
	flag "github.com/spf13/pflag"
)

// Label describes modification applied to object's labels.
// It takes a form of csv string slice, where values can have the following formats:
// k=v - format means that label should be added.
// k-  - format means that label should be removed.
// Because of that, the '=' is reserved and cannot be a part of the label.
type Label struct {
	add    map[string]string
	remove []string
}

var _ flag.Value = (*Label)(nil)

func (l *Label) String() string {
	var parts []string
	for k, v := range l.add {
		parts = append(parts, k+"="+v)
	}
	for _, k := range l.remove {
		parts = append(parts, k+"-")
	}
	return strings.Join(parts, ",")
}

// Set implements pflag.Value.
func (l *Label) Set(s string) error {
	if s == "" {
		return nil
	}
	stringReader := strings.NewReader(s)
	csvReader := csv.NewReader(stringReader)
	spec, err := csvReader.Read()
	if err != nil {
		return fmt.Errorf("parse csv label list: %w", err)
	}
	// Taken from https://github.com/kubernetes/kubectl/blob/master/pkg/cmd/label/label.go#L411
	add := map[string]string{}
	var remove []string
	for _, labelSpec := range spec {
		switch {
		case strings.Contains(labelSpec, "="):
			parts := strings.Split(labelSpec, "=")
			if len(parts) != 2 {
				return fmt.Errorf("%w: %v", errUnknownLabelSpec, labelSpec)
			}
			add[parts[0]] = parts[1]
		case strings.HasSuffix(labelSpec, "-"):
			remove = append(remove, labelSpec[:len(labelSpec)-1])
		default:
			return fmt.Errorf("%w: %v", errUnknownLabelSpec, labelSpec)
		}
	}
	for _, removeLabel := range remove {
		if _, found := add[removeLabel]; found {
			return errAddAndRemoveLabel
		}
	}
	l.add = add
	l.remove = remove
	return nil
}

var (
	errUnknownLabelSpec  = errors.New("unknown label spec, label must be formatted as either key=value, or key-")
	errAddAndRemoveLabel = errors.New("can not both modify and remove a label in the same command")
)

// Type implements pflag.Value.
func (l *Label) Type() string {
	return "map"
}

// ApplyDiff adds and removes specified labels.
func (l *Label) ApplyDiff(labels map[string]string) map[string]string {
	out := maps.Clone(labels)
	if l == nil {
		return out
	}
	if out == nil {
		out = make(map[string]string)
	}

	for k, v := range l.add {
		out[k] = v
	}
	for _, k := range l.remove {
		delete(out, k)
	}

	return out
}

// NewLabels returns newly added labels.
func (l *Label) NewLabels() map[string]string {
	if l.add == nil {
		return map[string]string{}
	}
	return maps.Clone(l.add)
}

// Cron wraps string for early validation.
type Cron struct {
	v string
}

var _ flag.Value = (*Cron)(nil)

func (c *Cron) String() string {
	return c.v
}

// Set implements pflag.Value.
func (c *Cron) Set(s string) error {
	p := cron.NewParser(cron.SecondOptional | cron.Minute | cron.Hour | cron.Dom | cron.Month | cron.Dow | cron.Descriptor)
	if _, err := p.Parse(s); err != nil {
		return err
	}

	c.v = s
	return nil
}

func (c *Cron) Value() string {
	return c.v
}

// Type implements pflag.Value.
func (c *Cron) Type() string {
	return "string"
}

// Timezone wraps string for early validation.
type Timezone struct {
	v string
}

var _ flag.Value = (*Timezone)(nil)

func (c *Timezone) String() string {
	return c.v
}

// Set implements pflag.Value.
func (c *Timezone) Set(s string) error {
	if _, err := time.LoadLocation(s); err != nil {
		return err
	}

	c.v = s
	return nil
}

func (c *Timezone) Value() string {
	return c.v
}

// Type implements pflag.Value.
func (c *Timezone) Type() string {
	return "string"
}

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
	t.v, err = parserTime(s, false)
	return
}

// If zeroNow is set to true, all strings representing 'now' are parsed to zero value.
// Note that other strings containing 'now' (e.g. 'now+2h') are always parsed normally.
func parserTime(s string, zeroNow bool) (time.Time, error) {
	if strings.HasPrefix(s, "now") {
		now := timeutc.Now()
		if s == "now" {
			if zeroNow {
				return time.Time{}, nil
			}

			return now, nil
		}
		d, err := duration.ParseDuration(s[3:])
		if err != nil {
			return time.Time{}, err
		}

		if zeroNow && d.Duration().Microseconds() == 0 {
			return time.Time{}, nil
		}

		return now.Add(d.Duration()), nil
	}

	t, err := timeutc.Parse(time.RFC3339, s)
	if err != nil {
		return time.Time{}, err
	}
	return t.UTC(), nil
}

func (t *Time) Value() time.Time {
	return t.v
}

// DateTimePtr converts Time to OpenAPI DateTime pointer.
func (t *Time) DateTimePtr() *strfmt.DateTime {
	if t.v.IsZero() {
		return nil
	}
	v := strfmt.DateTime(t.v)
	return &v
}

// Type implements pflag.Value.
func (t *Time) Type() string {
	return "string"
}

// StartDate wraps time.Time and add support for now+duration syntax.
// The difference between StartDate and Time is that StartDate parses pure 'now' into zero value.
// That's because in case of '--start-date now' we want to calculate 'now'
// at the time of task scheduling, NOT when parsing command arguments.
type StartDate struct {
	v time.Time
}

var _ flag.Value = (*StartDate)(nil)

func (d *StartDate) String() string {
	if d.v.IsZero() {
		return ""
	}
	return d.v.String()
}

// Set implements pflag.Value.
func (d *StartDate) Set(s string) (err error) {
	d.v, err = parserTime(s, true)
	return
}

func (d *StartDate) Value() time.Time {
	return d.v
}

// DateTimePtr converts Time to OpenAPI DateTime pointer.
func (d *StartDate) DateTimePtr() *strfmt.DateTime {
	if d.v.IsZero() {
		return nil
	}
	v := strfmt.DateTime(d.v)
	return &v
}

// Type implements pflag.Value.
func (d *StartDate) Type() string {
	return "string"
}

// Duration wraps duration.Duration that adds support for days to time.Duration.
type Duration struct {
	v duration.Duration
}

// DurationWithDefault returns new Duration initialized to a given value.
func DurationWithDefault(d time.Duration) Duration {
	return Duration{
		v: duration.Duration(d),
	}
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
	errValidation := errors.New("intensity must be a non-negative integer")

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
