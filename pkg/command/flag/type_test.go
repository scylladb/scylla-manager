// Copyright (C) 2017 ScyllaDB

package flag

import (
	"strings"
	"testing"
	"time"

	"github.com/scylladb/scylla-manager/v3/pkg/util/timeutc"
)

func TestParseTime(t *testing.T) {
	table := []struct {
		S  string
		T  time.Time
		ZN bool
		E  string
	}{
		{
			S: "now",
			T: timeutc.Now(),
		},
		{
			S:  "now",
			T:  time.Time{},
			ZN: true,
		},
		{
			S:  "now+0h",
			T:  time.Time{},
			ZN: true,
		},
		{
			S: "now+1h",
			T: timeutc.Now().Add(time.Hour),
		},
		{
			S:  "now+2h",
			T:  timeutc.Now().Add(2 * time.Hour),
			ZN: true,
		},
		{
			S: timeutc.Now().Add(time.Hour).Format(time.RFC3339),
			T: timeutc.Now().Add(time.Hour),
		},
		{
			S: "2019-05-02T15:04:05Z07:00",
			E: "extra text",
		},
	}

	const epsilon = 5 * time.Second

	for i, test := range table {
		m, err := parserTime(test.S, test.ZN)

		msg := ""
		if err != nil {
			msg = err.Error()
		}
		if test.E != "" || msg != "" {
			if !strings.Contains(msg, test.E) {
				t.Error(i, msg)
			}
			if msg != "" && test.E == "" {
				t.Error(i, msg)
			}
			continue
		}

		diff := test.T.Sub(m).Abs()
		if diff > epsilon {
			t.Fatal(i, m, test.T, diff, test.S)
		}
	}
}
