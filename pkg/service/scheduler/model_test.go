// Copyright (C) 2017 ScyllaDB

package scheduler

import (
	"testing"
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
