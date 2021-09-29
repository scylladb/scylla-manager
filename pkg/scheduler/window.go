// Copyright (C) 2017 ScyllaDB

package scheduler

import (
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/pkg/errors"
)

// EachDay is a special weekday marker that matches any day.
const EachDay = time.Weekday(-1)

// WeekdayTime specifies weekday and time in that day.
// The time must be less than 24h.
type WeekdayTime struct {
	Weekday time.Weekday
	Time    time.Duration
}

var (
	weekdayTimeRegexp = regexp.MustCompile("(?i)^((Mon|Tue|Wed|Thu|Fri|Sat|Sun)-)?([0-9]{1,2}):([0-9]{2})$")
	weekdayRev        = map[string]time.Weekday{
		"":    EachDay,
		"mon": time.Monday,
		"tue": time.Tuesday,
		"wed": time.Wednesday,
		"thu": time.Thursday,
		"fri": time.Friday,
		"sat": time.Saturday,
		"sun": time.Sunday,
	}
)

func (i *WeekdayTime) UnmarshalText(text []byte) error {
	m := weekdayTimeRegexp.FindSubmatch(text)
	if len(m) == 0 {
		return errors.New("invalid format")
	}
	var wdt WeekdayTime

	w, ok := weekdayRev[strings.ToLower(string(m[2]))]
	if !ok {
		return errors.Errorf("unknown day of week %q", string(m[2]))
	}
	wdt.Weekday = w

	hh, _ := strconv.Atoi(string(m[3])) // nolint: errcheck
	if hh >= 24 {
		return errors.Errorf("invalid hour %d", hh)
	}
	mm, _ := strconv.Atoi(string(m[4])) // nolint: errcheck
	if mm >= 60 {
		return errors.Errorf("invalid minute %d", mm)
	}
	wdt.Time = time.Duration(hh*60+mm) * time.Minute

	*i = wdt
	return nil
}

const day = 24 * time.Hour

// Next returns the closest time after now that matches the weekday and time.
// It is Location aware, the same time in different locations will have
// different results.
func (i WeekdayTime) Next(now time.Time) time.Time {
	t := time.Date(now.Year(), now.Month(), now.Day(), 0, 0, 0, 0, now.Location())
	w := i.Weekday - t.Weekday()
	if w < 0 || w == 0 && now.Sub(t) > i.Time {
		w += 7
	}
	return t.Add(time.Duration(w)*day + i.Time)
}

func (i WeekdayTime) index() int64 {
	d := day*time.Duration(i.Weekday) + i.Time
	return int64(d)
}

func (i WeekdayTime) validate() error {
	if i.Time >= 24*time.Hour {
		return errors.New("time must be less than 24h")
	}
	return nil
}

type slot struct {
	Begin WeekdayTime
	End   WeekdayTime
}

// Window specifies repeatable time windows when scheduler can run a function.
// When window ends the scheduler schedules a continuation in a next window.
type Window []slot

func NewWindow(wdt ...WeekdayTime) (Window, error) {
	if len(wdt) == 0 {
		return nil, errors.New("empty")
	}
	if len(wdt)%2 != 0 {
		return nil, errors.New("number of points must be even")
	}
	for i := range wdt {
		if err := wdt[i].validate(); err != nil {
			return nil, errors.Wrapf(err, "invalid value at pos %d", i)
		}
	}

	w := make(Window, len(wdt)/2)
	for i := range w {
		w[i].Begin = wdt[2*i]
		w[i].End = wdt[2*i+1]
		if w[i].Begin.index() >= w[i].End.index() {
			return nil, errors.Errorf("start at pos %d after stop at pos %d", 2*i, 2*i+1)
		}
	}

	sort.Slice(w, func(i, j int) bool {
		return w[i].Begin.index() < w[j].Begin.index()
	})

	for i := 1; i < len(w); i++ {
		b := w[i].Begin.index()
		e := w[i-1].End.index()
		if b <= e {
			return nil, errors.New("slots overlap")
		}
	}

	return w, nil
}

// Next returns the closest open slot begin and end time given now value.
// The end time is always > now, begin may be before now in case now is
// inside an open slot.
func (w Window) Next(now time.Time) (begin, end time.Time) {
	if w == nil {
		return now, time.Time{}
	}

	// To find the smallest value of w[i].End.Time(now) over i we use
	// binary search on a helper function that compares n-th value with 0-th
	// value. The calculated values are sorted but may be shifted.
	//
	// Ex 1, now=W
	//
	// M | T | W | T | F | S | S
	// --+---+---+---+---+---+--
	// 5 | 6 | 0 | 1 | 2 | 3 | 4 (values)
	// 0 | 0 | 1 | 1 | 1 | 1 | 1 (indicator)
	//
	// Ex 2, now=S
	// --+---+---+---+---+---+--
	// 1 | 2 | 3 | 4 | 5 | 6 | 0 (values)
	// 0 | 0 | 0 | 0 | 0 | 0 | 1 (indicator)

	u0 := w[0].End.Next(now).Unix()
	i := sort.Search(len(w), func(i int) bool {
		u := w[i].End.Next(now).Unix()
		return u < u0
	})
	if i == len(w) {
		i = 0
	}

	begin = w[i].Begin.Next(now)
	end = w[i].End.Next(now)

	if begin.After(end) {
		begin = now
	}
	return // nolint: nakedret
}
