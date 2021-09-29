// Copyright (C) 2017 ScyllaDB

package scheduler

import (
	"strings"
	"testing"
	"time"
)

func TestWeekdayTime(t *testing.T) {
	const (
		m = time.Minute
		h = time.Hour
	)

	table := []struct {
		Name        string
		WeekdayTime WeekdayTime
		Now         time.Time
		Golden      time.Time
	}{
		{
			Name: "today before",
			WeekdayTime: WeekdayTime{
				Weekday: time.Friday,
				Time:    4*h + 5*m,
			},
			Now:    time.Date(2021, 9, 24, 3, 5, 0, 0, time.UTC),
			Golden: time.Date(2021, 9, 24, 4, 5, 0, 0, time.UTC),
		},
		{
			Name: "today after",
			WeekdayTime: WeekdayTime{
				Weekday: time.Friday,
				Time:    4*h + 5*m,
			},
			Now:    time.Date(2021, 9, 24, 5, 5, 0, 0, time.UTC),
			Golden: time.Date(2021, 10, 1, 4, 5, 0, 0, time.UTC),
		},
		{
			Name: "this week",
			WeekdayTime: WeekdayTime{
				Weekday: time.Friday,
				Time:    4*h + 5*m,
			},
			Now:    time.Date(2021, 9, 23, 15, 59, 0, 0, time.UTC),
			Golden: time.Date(2021, 9, 24, 4, 5, 0, 0, time.UTC),
		},
		{
			Name: "next week",
			WeekdayTime: WeekdayTime{
				Weekday: time.Monday,
				Time:    4*h + 5*m,
			},
			Now:    time.Date(2021, 9, 23, 15, 59, 0, 0, time.UTC),
			Golden: time.Date(2021, 9, 27, 4, 5, 0, 0, time.UTC),
		},
		{
			Name: "leap years",
			WeekdayTime: WeekdayTime{
				Weekday: time.Saturday,
				Time:    4*h + 5*m,
			},
			Now:    time.Date(2020, 2, 23, 15, 59, 0, 0, time.UTC),
			Golden: time.Date(2020, 2, 29, 4, 5, 0, 0, time.UTC),
		},
	}

	for i := range table {
		test := table[i]
		t.Run(test.Name, func(t *testing.T) {
			if s := test.WeekdayTime.Next(test.Now); s != test.Golden {
				t.Fatalf("WeekdayTime.Next()=%s, expected %s, diff %s", s, test.Golden, time.Duration(s.UnixNano()-test.Golden.UnixNano()))
			}
		})
	}
}

func TestWeekdayTimeUnmarshalText(t *testing.T) {
	table := []struct {
		Str    string
		Golden WeekdayTime
	}{
		{
			Str: "Mon-9:20",
			Golden: WeekdayTime{
				Weekday: time.Monday,
				Time:    time.Duration(9*60+20) * time.Minute,
			},
		},
		{
			Str: "Mon-09:20",
			Golden: WeekdayTime{
				Weekday: time.Monday,
				Time:    time.Duration(9*60+20) * time.Minute,
			},
		},
		{
			Str: "19:00",
			Golden: WeekdayTime{
				Weekday: EachDay,
				Time:    time.Duration(19*60) * time.Minute,
			},
		},
	}

	for i := range table {
		test := table[i]
		t.Run(test.Str, func(t *testing.T) {
			var wdt WeekdayTime
			if err := wdt.UnmarshalText([]byte(test.Str)); err != nil {
				t.Fatalf("UnmarshalText() error %s", err)
			}
			if wdt != test.Golden {
				t.Fatalf("Have %v, expected %v", wdt, test.Golden)
			}
		})
	}
}

func TestWeekdayTimeUnmarshalTextError(t *testing.T) {
	table := []struct {
		Name string
		Str  string
		Err  string
	}{
		{
			Str: "Foo-9:20",
			Err: "invalid format",
		},
		{
			Str: "Mon-24:20",
			Err: "invalid hour",
		},
		{
			Str: "Mon-100:20",
			Err: "invalid format",
		},
		{
			Str: "Mon-05:60",
			Err: "invalid minute",
		},
		{
			Str: "Mon-05:100",
			Err: "invalid format",
		},
		{
			Str: "05:60",
			Err: "invalid minute",
		},
		{
			Str: "05:100",
			Err: "invalid format",
		},
	}

	for i := range table {
		test := table[i]
		t.Run(test.Str, func(t *testing.T) {
			var wdt WeekdayTime
			err := wdt.UnmarshalText([]byte(test.Str))
			t.Log(err)
			if err == nil || !strings.Contains(err.Error(), test.Err) {
				t.Fatalf("UnmarshalText() error %s, expected to contain %s", err, test.Err)
			}
		})
	}
}

func TestWindowValidate(t *testing.T) {
	table := []struct {
		Name     string
		Instants []WeekdayTime
		Error    string
	}{
		{
			Name: "Odd nr. of instances",
			Instants: []WeekdayTime{
				{
					Weekday: time.Monday,
				},
			},
			Error: "number of points must be even",
		},
		{
			Name: "Begin end mismatch",
			Instants: []WeekdayTime{
				{
					Weekday: time.Tuesday,
				},
				{
					Weekday: time.Monday,
				},
			},
			Error: "after stop",
		},
		{
			Name: "Begin end equal",
			Instants: []WeekdayTime{
				{
					Weekday: time.Monday,
				},
				{
					Weekday: time.Monday,
				},
			},
			Error: "after stop",
		},
		{
			Name: "Overlap",
			Instants: []WeekdayTime{
				{
					Weekday: time.Monday,
				},
				{
					Weekday: time.Wednesday,
				},
				{
					Weekday: time.Tuesday,
				},
				{
					Weekday: time.Friday,
				},
			},
			Error: "overlap",
		},
	}

	for i := range table {
		test := table[i]
		t.Run(test.Name, func(t *testing.T) {
			if _, err := NewWindow(test.Instants...); err == nil || !strings.Contains(err.Error(), test.Error) {
				t.Fatalf("NewWindow() error %s, expected %s", err, test.Error)
			}
		})
	}
}

func TestWindowNext(t *testing.T) {
	ins := []WeekdayTime{
		{
			Weekday: time.Monday,
		},
		{
			Weekday: time.Monday,
			Time:    500 * time.Second,
		},
		{
			Weekday: time.Wednesday,
		},
		{
			Weekday: time.Wednesday,
			Time:    500 * time.Second,
		},
		{
			Weekday: time.Friday,
		},
		{
			Weekday: time.Friday,
			Time:    500 * time.Second,
		},
	}

	w, err := NewWindow(ins...)
	if err != nil {
		t.Fatalf("NewWindow() error %s", err)
	}

	table := []struct {
		Name  string
		Now   time.Time
		Begin time.Time
		End   time.Time
	}{
		{
			Name:  "Sun",
			Now:   time.Date(2021, 9, 19, 0, 0, 0, 0, time.UTC),
			Begin: time.Date(2021, 9, 20, 0, 0, 0, 0, time.UTC),
			End:   time.Date(2021, 9, 20, 0, 0, 0, 0, time.UTC).Add(500 * time.Second),
		},
		{
			Name:  "Mon",
			Now:   time.Date(2021, 9, 20, 0, 5, 0, 0, time.UTC),
			Begin: time.Date(2021, 9, 20, 0, 5, 0, 0, time.UTC),
			End:   time.Date(2021, 9, 20, 0, 0, 0, 0, time.UTC).Add(500 * time.Second),
		},
		{
			Name:  "Tue",
			Now:   time.Date(2021, 9, 21, 0, 0, 0, 0, time.UTC),
			Begin: time.Date(2021, 9, 22, 0, 0, 0, 0, time.UTC),
			End:   time.Date(2021, 9, 22, 0, 0, 0, 0, time.UTC).Add(500 * time.Second),
		},
		{
			Name:  "Wed",
			Now:   time.Date(2021, 9, 22, 0, 5, 0, 0, time.UTC),
			Begin: time.Date(2021, 9, 22, 0, 5, 0, 0, time.UTC),
			End:   time.Date(2021, 9, 22, 0, 0, 0, 0, time.UTC).Add(500 * time.Second),
		},
		{
			Name:  "Thu",
			Now:   time.Date(2021, 9, 23, 0, 0, 0, 0, time.UTC),
			Begin: time.Date(2021, 9, 24, 0, 0, 0, 0, time.UTC),
			End:   time.Date(2021, 9, 24, 0, 0, 0, 0, time.UTC).Add(500 * time.Second),
		},
	}

	for i := range table {
		test := table[i]
		t.Run(test.Name, func(t *testing.T) {
			b, e := w.Next(test.Now)
			if b != test.Begin {
				t.Errorf("Next()=%s, expected %s, diff %s", b, test.Begin, time.Duration(b.UnixNano()-test.Begin.UnixNano()))
			}
			if e != test.End {
				t.Errorf("Next()=%s, expected %s, diff %s", e, test.End, time.Duration(e.UnixNano()-test.End.UnixNano()))
			}
		})
	}
}
