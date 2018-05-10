// Copyright (C) 2017 ScyllaDB

package main

import (
	"strings"
	"testing"
	"time"

	"github.com/go-openapi/strfmt"
	"github.com/scylladb/mermaid/internal/timeutc"
)

func TestParseStartDate(t *testing.T) {
	const epsilon = 50 * time.Millisecond

	now := timeutc.Now()
	nowSafe := now.Add(nowSafety)

	table := []struct {
		S string
		T time.Time
		E string
	}{
		{
			S: "now",
			T: nowSafe,
		},
		{
			S: "now-5s",
			E: "start date cannot be in the past"},
		{
			S: "now+5s",
			E: "start date must be at least in",
		},
		{
			S: "now+1h",
			T: now.Add(time.Hour),
		},
		{
			S: now.Add(-5 * time.Second).Format(time.RFC3339),
			E: "start date cannot be in the past"},
		{
			S: now.Add(5 * time.Second).Format(time.RFC3339),
			E: "start date must be at least in",
		},
		{
			S: now.Add(time.Hour).Format(time.RFC3339),
			T: now.Add(time.Hour),
		},
	}

	for i, test := range table {
		startDate, err := parseStartDate(test.S)

		msg := ""
		if err != nil {
			msg = err.Error()
		}
		if test.E != "" || msg != "" {
			if !strings.Contains(msg, test.E) {
				t.Error(i, msg)
			}
			continue
		}

		diff := time.Time(startDate).Sub(test.T)
		if diff < 0 {
			diff *= -1
		}
		if diff > epsilon {
			t.Fatal(i, startDate, test.T, diff)
		}
	}
}

func TestFormatTimeZero(t *testing.T) {
	if s := formatTime(strfmt.DateTime(time.Time{})); s != "" {
		t.Error(s)
	}
}

func TestFormatTimeNonZero(t *testing.T) {
	tz, _ := timeutc.Now().Local().Zone()

	if s := formatTime(strfmt.DateTime(timeutc.Now())); !strings.Contains(s, tz) {
		t.Error(s)
	}
}
