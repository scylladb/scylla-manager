// Copyright (C) 2026 ScyllaDB

package scheduler

import (
	"encoding/json"
	"strconv"
	"strings"

	"github.com/scylladb/scylla-manager/v3/pkg/metrics"
)

// taskInfoProperties is the subset of task properties reported by the
// "task_info" metric. The JSON field names mirror the properties of the
// repair and backup tasks (see pkg/service/repair and pkg/service/backup),
// and are decoded here instead of being imported, so that the scheduler does
// not depend on the services it runs. Fields missing from the properties stay
// empty, so that the metric reports only what was actually configured, never
// a default.
//
// "keyspace" and "dc" are configured by both task types, the remaining fields
// belong to one of them - a field that the task type does not have simply
// never appears in its properties.
type taskInfoProperties struct {
	Keyspace []string `json:"keyspace"`
	DC       []string `json:"dc"`

	// Repair.
	KeyspaceReplication string `json:"keyspace_replication"`
	IncrementalMode     string `json:"incremental_mode"`
	Host                string `json:"host"`
	FailFast            *bool  `json:"fail_fast"`

	// Backup. Location decodes into strings because backupspec.Location
	// marshals itself as "[dc:]<provider>:<path>", which is also how sctool
	// prints it.
	Location          []string `json:"location"`
	Retention         *int     `json:"retention"`
	RetentionDays     *int     `json:"retention_days"`
	Method            string   `json:"method"`
	PurgeOnly         *bool    `json:"purge_only"`
	SkipSchema        *bool    `json:"skip_schema"`
	RetentionLockMode string   `json:"retention_lock_mode"`
}

// newTaskInfo describes a task for the "task_info" metric.
// Properties that cannot be decoded are skipped - the metric is
// informational, and a task with unreadable properties should still be
// listed with its name and type.
func newTaskInfo(t *Task) metrics.TaskInfo {
	info := metrics.TaskInfo{Name: t.Name, Cron: taskSchedule(t)}

	var p taskInfoProperties
	if len(t.Properties) > 0 {
		if err := json.Unmarshal(t.Properties, &p); err != nil {
			// Nothing can be said about the properties, so don't pretend that
			// the defaults apply either.
			return info
		}
	}

	info.Keyspace = strings.Join(p.Keyspace, ",")
	info.DC = strings.Join(p.DC, ",")

	info.KeyspaceReplication = p.KeyspaceReplication
	info.IncrementalMode = p.IncrementalMode
	info.Host = p.Host
	info.FailFast = formatBool(p.FailFast)

	info.Location = strings.Join(p.Location, ",")
	info.Retention = formatInt(p.Retention)
	info.RetentionDays = formatInt(p.RetentionDays)
	info.Method = p.Method
	info.PurgeOnly = formatBool(p.PurgeOnly)
	info.SkipSchema = formatBool(p.SkipSchema)
	info.RetentionLockMode = p.RetentionLockMode

	switch t.Type {
	case RepairTask:
		applyRepairDefaults(&info)
	case BackupTask:
		applyBackupDefaults(&info, p)
	}
	return info
}

func formatBool(v *bool) string {
	if v == nil {
		return ""
	}
	return strconv.FormatBool(*v)
}

func formatInt(v *int) string {
	if v == nil {
		return ""
	}
	return strconv.Itoa(*v)
}

// Defaults applied to a repair task property that was not configured, so
// that the metric describes what the task will actually do rather than what
// was typed. They mirror defaultTaskProperties in pkg/service/repair, which
// cannot be imported here without making the scheduler depend on the
// services it runs - keep the two in step.
//
// "all" is not a literal default: an empty dc or host filter means every
// datacenter or host. An empty incremental mode means that SM does not send
// the parameter and Scylla applies its own default, which is incremental.
const (
	defaultRepairKeyspace            = "*,!system_traces"
	defaultRepairKeyspaceReplication = "all"
	defaultRepairIncrementalMode     = "incremental"
	defaultRepairDC                  = "all"
	defaultRepairHost                = "all"
	defaultRepairFailFast            = "false"
)

func applyRepairDefaults(info *metrics.TaskInfo) {
	for _, f := range []struct {
		v   *string
		def string
	}{
		{&info.Keyspace, defaultRepairKeyspace},
		{&info.KeyspaceReplication, defaultRepairKeyspaceReplication},
		{&info.IncrementalMode, defaultRepairIncrementalMode},
		{&info.DC, defaultRepairDC},
		{&info.Host, defaultRepairHost},
		{&info.FailFast, defaultRepairFailFast},
	} {
		if *f.v == "" {
			*f.v = f.def
		}
	}
}

// Defaults applied to a backup task property that was not configured. They
// mirror defaultTaskProperties and defaultRetention in pkg/service/backup,
// which cannot be imported here for the same reason as the repair ones -
// keep the two in step.
//
// "all" is not a literal default: an empty keyspace or dc filter means every
// keyspace or datacenter.
const (
	defaultBackupKeyspace          = "all"
	defaultBackupDC                = "all"
	defaultBackupMethod            = "rclone"
	defaultBackupPurgeOnly         = "false"
	defaultBackupSkipSchema        = "false"
	defaultBackupRetentionLockMode = "disabled"
	// Retention and retention days are not independent: SM falls back to
	// keeping 3 backups only when neither is configured, and otherwise takes
	// the one that is set and treats the other as 0.
	defaultBackupRetention     = "3"
	defaultBackupRetentionDays = "0"
)

func applyBackupDefaults(info *metrics.TaskInfo, p taskInfoProperties) {
	if p.Retention == nil && p.RetentionDays == nil {
		info.Retention = defaultBackupRetention
		info.RetentionDays = defaultBackupRetentionDays
	} else {
		if p.Retention == nil {
			info.Retention = "0"
		}
		if p.RetentionDays == nil {
			info.RetentionDays = "0"
		}
	}

	for _, f := range []struct {
		v   *string
		def string
	}{
		{&info.Keyspace, defaultBackupKeyspace},
		{&info.DC, defaultBackupDC},
		{&info.Method, defaultBackupMethod},
		{&info.PurgeOnly, defaultBackupPurgeOnly},
		{&info.SkipSchema, defaultBackupSkipSchema},
		{&info.RetentionLockMode, defaultBackupRetentionLockMode},
	} {
		if *f.v == "" {
			*f.v = f.def
		}
	}
}

// taskSchedule describes when a task is expected to run. It reports the cron
// specification, falling back to the deprecated interval, and stays empty for
// a task that is not scheduled at all.
func taskSchedule(t *Task) string {
	if !t.Sched.Cron.IsZero() {
		return t.Sched.Cron.Spec
	}
	if t.Sched.Interval != 0 {
		return t.Sched.Interval.String()
	}
	// Named rather than left empty, both because "not scheduled" is a
	// property of the task and not a missing value, and because it sorts
	// after any cron specification, which keeps ad-hoc tasks at the end of
	// a list ordered by schedule.
	return metrics.TaskScheduleAdHoc
}
