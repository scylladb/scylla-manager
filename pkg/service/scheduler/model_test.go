// Copyright (C) 2017 ScyllaDB

package scheduler

import (
	"testing"
	"time"
)

func TestTaskType(t *testing.T) {
	allTaskTypes := []TaskType{
		UnknownTask,
		BackupTask,
		HealthCheckTask,
		RepairTask,
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
	spec := "@every 15s"

	var cron Cron
	if err := cron.UnmarshalText([]byte(spec)); err != nil {
		t.Fatal(err)
	}
	b, _ := cron.MarshalText()
	if string(b) != spec {
		t.Fatalf("MarshalText() = %s, expected %s", string(b), spec)
	}
}

func TestNewCronEvery(t *testing.T) {
	c := NewCronEvery(15 * time.Second)
	if c.IsZero() {
		t.Fatal()
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
	if v != l {
		t.Fatalf("UnmarshalText() = %s, expected %s", v, l)
	}
}
