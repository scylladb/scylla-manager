// Copyright (C) 2017 ScyllaDB

package flag

import (
	"time"

	"github.com/scylladb/scylla-manager/pkg/managerclient"
	"github.com/scylladb/scylla-manager/pkg/util/timeutc"
	"github.com/spf13/cobra"
)

// TaskBase handles common task schedule related flags.
type TaskBase struct {
	cobra.Command
	update bool

	enabled    bool
	name       string
	cron       Cron
	window     []string
	timezone   Timezone
	interval   Duration
	startDate  Time
	numRetries int
	retryWait  Duration
}

func MakeTaskBase() TaskBase {
	return TaskBase{
		timezone:  Timezone{timeutc.LocalName},
		retryWait: DurationWithDefault(10 * time.Minute),
	}
}

func NewUpdateTaskBase() TaskBase {
	base := MakeTaskBase()
	base.Command.Args = cobra.MaximumNArgs(1)
	base.update = true
	return base
}

func (cmd *TaskBase) Init() {
	w := Wrap(cmd.Flags())
	w.enabled(&cmd.enabled)
	w.name(&cmd.name)
	w.cron(&cmd.cron)
	w.window(&cmd.window)
	w.timezone(&cmd.timezone)
	w.interval(&cmd.interval)
	w.startDate(&cmd.startDate)
	w.numRetries(&cmd.numRetries, cmd.numRetries)
	w.retryWait(&cmd.retryWait)
}

// Update allows differentiating instances created with NewUpdateTaskBase.
func (cmd *TaskBase) Update() bool {
	return cmd.update
}

// CreateTask creates a task scaffold with common task properties preset.
func (cmd *TaskBase) CreateTask(taskType string) *managerclient.Task {
	return &managerclient.Task{
		Type:    taskType,
		Enabled: cmd.enabled,
		Name:    cmd.name,
		Schedule: &managerclient.Schedule{
			Cron:       cmd.cron.Value(),
			Window:     cmd.window,
			Timezone:   cmd.timezone.Value(),
			Interval:   cmd.interval.String(),
			StartDate:  cmd.startDate.DateTimePtr(),
			NumRetries: int64(cmd.numRetries),
			RetryWait:  cmd.retryWait.String(),
		},
		Properties: make(map[string]interface{}),
	}
}

// UpdateTask updates task fields if flags are set, returns true if there are changes.
func (cmd *TaskBase) UpdateTask(task *managerclient.Task) bool {
	ok := false
	if cmd.Flag("enabled").Changed {
		task.Enabled = cmd.enabled
		ok = true
	}
	if cmd.Flag("name").Changed {
		task.Name = cmd.name
		ok = true
	}
	if cmd.Flag("cron").Changed {
		task.Schedule.Cron = cmd.cron.Value()
		ok = true
	}
	if cmd.Flag("window").Changed {
		task.Schedule.Window = cmd.window
		ok = true
	}
	if cmd.Flag("timezone").Changed {
		task.Schedule.Timezone = cmd.timezone.Value()
		ok = true
	}
	if cmd.Flag("interval").Changed {
		task.Schedule.Interval = cmd.interval.String()
		ok = true
	}
	if cmd.Flag("start-date").Changed {
		task.Schedule.StartDate = cmd.startDate.DateTimePtr()
		ok = true
	}
	if cmd.Flag("num-retries").Changed {
		task.Schedule.NumRetries = int64(cmd.numRetries)
		ok = true
	}
	if cmd.Flag("retry-wait").Changed {
		task.Schedule.RetryWait = cmd.retryWait.String()
		ok = true
	}
	return ok
}
