// Copyright (C) 2017 ScyllaDB

package scheduler

import (
	"testing"
	"time"

	"github.com/cenkalti/backoff/v4"
	"github.com/scylladb/scylla-manager/v3/pkg/util/duration"
	"github.com/scylladb/scylla-manager/v3/pkg/util/timeutc"
)

func TestTaskType(t *testing.T) {
	allTaskTypes := []TaskType{
		UnknownTask,
		BackupTask,
		HealthCheckTask,
		RepairTask,
		SuspendTask,
		ValidateBackupTask,
	}

	for _, golden := range allTaskTypes {
		t.Run(golden.String(), func(t *testing.T) {
			text, err := golden.MarshalText()
			if err != nil {
				t.Fatal("MarshalText() error", err)
			}
			var v TaskType
			if err := v.UnmarshalText(text); err != nil {
				t.Fatal("UnmarshalText() error", err)
			}
			if v != golden {
				t.Fatal(v)
			}
		})
	}
}

func TestStatus(t *testing.T) {
	for _, golden := range allStatuses {
		t.Run(golden.String(), func(t *testing.T) {
			text, err := golden.MarshalText()
			if err != nil {
				t.Fatal("MarshalText() error", err)
			}
			var v Status
			if err := v.UnmarshalText(text); err != nil {
				t.Fatal("UnmarshalText() error", err)
			}
			if v != golden {
				t.Fatal(v)
			}
		})
	}
}

func TestCronMarshalUnmarshal(t *testing.T) {
	nonZeroTimeString := "2024-02-23T01:12:00Z"
	nonZeroTime, err := timeutc.Parse(time.RFC3339, nonZeroTimeString)
	if err != nil {
		t.Fatal(err)
	}

	for _, tc := range []struct {
		name         string
		data         []byte
		expectedSpec cronSpecification
	}{
		{
			name: "(3.2.6 backward compatibility) unmarshal spec",
			data: []byte("@every 15s"),
			expectedSpec: cronSpecification{
				Spec:      "@every 15s",
				StartDate: time.Time{},
			},
		},
		{
			name: "unmarshal spec full struct zero time",
			data: []byte(`{"spec": "@every 15s", "start_date": "0001-01-01T00:00:00Z"}`),
			expectedSpec: cronSpecification{
				Spec:      "@every 15s",
				StartDate: time.Time{},
			},
		},
		{
			name: "unmarshal spec full struct non-zero time",
			data: []byte(`{"spec": "@every 15s", "start_date": "` + nonZeroTimeString + `"}`),
			expectedSpec: cronSpecification{
				Spec:      "@every 15s",
				StartDate: nonZeroTime,
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var cron, finalCron Cron
			if err := cron.UnmarshalText(tc.data); err != nil {
				t.Fatal(err)
			}
			b, err := cron.MarshalText()
			if err != nil {
				t.Fatal(err)
			}
			if err := finalCron.UnmarshalText(b); err != nil {
				t.Fatal(err)
			}

			if finalCron.Spec != tc.expectedSpec.Spec {
				t.Fatalf("MarshalText() = %s, expected spec %s", finalCron.Spec, tc.expectedSpec.Spec)
			}
			if finalCron.StartDate != tc.expectedSpec.StartDate {
				t.Fatalf("MarshalText() = %s, expected startDate %s", finalCron.StartDate, tc.expectedSpec.StartDate)
			}

		})
	}
}

func TestNewCronEvery(t *testing.T) {
	c := NewCronEvery(15*time.Second, time.Time{})
	if c.IsZero() {
		t.Fatal()
	}
}

func TestNewCronWithNonZeroStartDate(t *testing.T) {
	for _, tc := range []struct {
		name                string
		nowRFC3339          string
		startDateRFC3339    string
		cronExpression      string
		expectedNextRFC3339 string
	}{
		{
			name:                "current time couple of rounds before start date",
			nowRFC3339:          "2024-01-01T03:00:00Z",
			startDateRFC3339:    "2024-02-23T03:00:00Z",
			cronExpression:      "0 2 * * *",
			expectedNextRFC3339: "2024-02-24T02:00:00Z",
		},
		{
			name:                "current time couple of rounds before start date",
			nowRFC3339:          "2024-01-01T03:00:00Z",
			startDateRFC3339:    "0000-01-01T00:00:00Z",
			cronExpression:      "0 2 * * *",
			expectedNextRFC3339: "2024-01-02T02:00:00Z",
		},
	} {
		parsedStart, err := timeutc.Parse(time.RFC3339, tc.startDateRFC3339)
		if err != nil {
			t.Fatal(err)
		}
		parsedExpected, err := timeutc.Parse(time.RFC3339, tc.expectedNextRFC3339)
		if err != nil {
			t.Fatal(err)
		}
		parsedNow, err := timeutc.Parse(time.RFC3339, tc.nowRFC3339)
		if err != nil {
			t.Fatal(err)
		}
		c, err := NewCron(tc.cronExpression, parsedStart)
		if err != nil {
			t.Fatal(err)
		}
		next := c.Next(parsedNow)
		if !next.Equal(parsedExpected) {
			t.Fatalf("expected next schedule %v, but got %v", parsedExpected, next)
		}
	}
}

func TestEmptyCron(t *testing.T) {
	var cron Cron
	if err := cron.UnmarshalText(nil); err != nil {
		t.Fatal(err)
	}
	cron.Next(now())
}

func TestLocationMarshalUnmarshal(t *testing.T) {
	l := location{time.Local}

	b, err := l.MarshalText()
	if err != nil {
		t.Fatal(err)
	}
	var v location
	if err := v.UnmarshalText(b); err != nil {
		t.Fatal(err)
	}
	if v.Location.String() != l.Location.String() {
		t.Fatalf("UnmarshalText() = %+#v, expected %+#v", v, l)
	}
}

func TestScheduleBackoff(t *testing.T) {
	s := Schedule{
		NumRetries: 3,
		RetryWait:  duration.Duration(10 * time.Second),
	}
	b := s.backoff()

	for i, g := range []time.Duration{10 * time.Second, 20 * time.Second, 40 * time.Second, backoff.Stop} {
		if v := b.NextBackOff(); v != g {
			t.Errorf("%d got %s expected %s", i, v, g)
		}
	}
}
