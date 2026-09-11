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
// repair task (see pkg/service/repair), and are decoded here instead of
// being imported, so that the scheduler does not depend on the services
// it runs. Fields missing from the properties stay empty, so that the
// metric reports only what was actually configured, never a default.
type taskInfoProperties struct {
	Keyspace            []string `json:"keyspace"`
	KeyspaceReplication string   `json:"keyspace_replication"`
	IncrementalMode     string   `json:"incremental_mode"`
	DC                  []string `json:"dc"`
	Host                string   `json:"host"`
	FailFast            *bool    `json:"fail_fast"`
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
	info.KeyspaceReplication = string(p.KeyspaceReplication)
	info.IncrementalMode = p.IncrementalMode
	info.DC = strings.Join(p.DC, ",")
	info.Host = p.Host
	if p.FailFast != nil {
		info.FailFast = strconv.FormatBool(*p.FailFast)
	}
	if t.Type == RepairTask {
		applyRepairDefaults(&info)
	}
	return info
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
