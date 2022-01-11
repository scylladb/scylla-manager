// Copyright (C) 2017 ScyllaDB

package managerclient

import "github.com/scylladb/go-set/strset"

// TaskType enumeration.
const (
	BackupTask         string = "backup"
	HealthCheckTask    string = "healthcheck"
	RepairTask         string = "repair"
	SuspendTask        string = "suspend"
	ValidateBackupTask string = "validate_backup"
)

// TasksTypes is a set of all known task types.
var TasksTypes = strset.New(
	BackupTask,
	HealthCheckTask,
	RepairTask,
	SuspendTask,
	ValidateBackupTask,
)

// Status enumeration.
const (
	TaskStatusNew      string = "NEW"
	TaskStatusRunning  string = "RUNNING"
	TaskStatusStopping string = "STOPPING"
	TaskStatusStopped  string = "STOPPED"
	TaskStatusWaiting  string = "WAITING"
	TaskStatusDone     string = "DONE"
	TaskStatusError    string = "ERROR"
	TaskStatusAborted  string = "ABORTED"
)
