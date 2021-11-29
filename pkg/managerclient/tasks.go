// Copyright (C) 2017 ScyllaDB

package managerclient

// TaskType enumeration.
const (
	BackupTask                string = "backup"
	HealthCheckAlternatorTask string = "healthcheck_alternator"
	HealthCheckCQLTask        string = "healthcheck"
	HealthCheckRESTTask       string = "healthcheck_rest"
	RepairTask                string = "repair"
	ValidateBackupTask        string = "validate_backup"
)
