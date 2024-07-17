// Copyright (C) 2017 ScyllaDB

package flag

import (
	"errors"
	"maps"
	"strings"
	"testing"
	"time"

	"github.com/scylladb/go-set/strset"
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

func TestParseLabel(t *testing.T) {
	testCases := []struct {
		S string
		T Label
		E error
	}{
		{
			S: "k1=v1",
			T: Label{
				add: map[string]string{
					"k1": "v1",
				},
			},
		},
		{
			S: "k1-",
			T: Label{
				remove: []string{"k1"},
			},
		},
		{
			S: "k1=v1,k2=v2,k3-,k4=v4",
			T: Label{
				add: map[string]string{
					"k1": "v1",
					"k2": "v2",
					"k4": "v4",
				},
				remove: []string{"k3"},
			},
		},
		{
			S: "k1=v1,with space=with-dash_underscore666",
			T: Label{
				add: map[string]string{
					"k1":         "v1",
					"with space": "with-dash_underscore666",
				},
			},
		},
		{
			S: "k1=v1,k1-",
			E: errAddAndRemoveLabel,
		},
		{
			S: "k1=v1=v2",
			E: errUnknownLabelSpec,
		},
		{
			S: "k1",
			E: errUnknownLabelSpec,
		},
	}

	parse := func(s string) (Label, error) {
		var l Label
		err := l.Set(s)
		return l, err
	}

	for i, tc := range testCases {
		l, err := parse(tc.S)

		if tc.E != nil {
			if !errors.Is(err, tc.E) {
				t.Fatalf("%d: expected error: %s, but got: %s", i, tc.E, err)
			}
			continue
		} else if err != nil {
			t.Fatalf("%d: expected no error, but got: %s", i, err)
		}

		if !maps.Equal(l.add, tc.T.add) {
			t.Fatalf("%d: expected added labels: %v, but got: %v", i, l.add, tc.T.add)
		}
		if !strset.New(l.remove...).IsEqual(strset.New(tc.T.remove...)) {
			t.Fatalf("%d: expected removed labels: %v, but got: %v", i, l.remove, tc.T.remove)
		}
	}
}
