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
		S string
		D time.Duration
		E string
	}{
		{
			S: "now",
			D: 0,
		},
		{
			S: "now+1h",
			D: time.Hour,
		},
		{
			S: timeutc.Now().Add(time.Hour).Format(time.RFC3339),
			D: time.Hour,
		},
		{
			S: "2019-05-02T15:04:05Z07:00",
			E: "extra text",
		},
	}

	const epsilon = 5 * time.Second

	for i, test := range table {
		m, err := parserTime(test.S)

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

		diff := timeutc.Now().Add(test.D).Sub(m)
		if diff < 0 {
			diff *= -1
		}
		if diff > epsilon {
			t.Fatal(i, m, test.D, diff, test.S)
		}
	}
}
