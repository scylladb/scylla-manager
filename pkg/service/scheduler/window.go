// Copyright (C) 2021 ScyllaDB

package scheduler

import (
	"encoding/json"

	"github.com/gocql/gocql"
	"github.com/pkg/errors"
	"github.com/scylladb/scylla-manager/pkg/scheduler"
)

// WeekdayTime adds CQL capabilities to scheduler.WeekdayTime.
type WeekdayTime struct {
	scheduler.WeekdayTime
}

func (w WeekdayTime) MarshalCQL(info gocql.TypeInfo) ([]byte, error) {
	return w.MarshalText()
}

func (w *WeekdayTime) UnmarshalCQL(info gocql.TypeInfo, data []byte) error {
	return w.UnmarshalText(data)
}

// Window adds scheduler.Window validation to JSON parsing.
type Window []WeekdayTime

func (w *Window) UnmarshalJSON(data []byte) error {
	var wdt []scheduler.WeekdayTime
	if err := json.Unmarshal(data, &wdt); err != nil {
		return errors.Wrap(err, "window")
	}
	if _, err := scheduler.NewWindow(wdt...); err != nil {
		return errors.Wrap(err, "window")
	}

	s := make([]WeekdayTime, len(wdt))
	for i := range wdt {
		s[i] = WeekdayTime{wdt[i]}
	}
	*w = s

	return nil
}

func (w Window) asSchedulerWindow() scheduler.Window {
	if len(w) == 0 {
		return nil
	}
	wdt := make([]scheduler.WeekdayTime, len(w))
	for i := range w {
		wdt[i] = w[i].WeekdayTime
	}
	sw, _ := scheduler.NewWindow(wdt...) // nolint: errcheck
	return sw
}
