// Copyright (C) 2017 ScyllaDB

package flag

import (
	"github.com/go-openapi/strfmt"
	"github.com/scylladb/scylla-manager/pkg/managerclient"
	"github.com/spf13/cobra"
)

// TaskBase handles common task schedule related flags.
type TaskBase struct {
	cobra.Command
	update bool

	enabled    bool
	interval   Duration
	startDate  Time
	numRetries int
}

func NewTaskBase() TaskBase {
	cmd := TaskBase{}
	cmd.init()
	return cmd
}

func NewUpdateTaskBase() TaskBase {
	cmd := TaskBase{
		Command: cobra.Command{
			Args: cobra.ExactArgs(1),
		},
		update: true,
	}
	cmd.init()
	return cmd
}

func (cmd *TaskBase) init() {
	w := Wrap(cmd.Flags())
	w.enabled(&cmd.enabled)
	w.interval(&cmd.interval)
	w.startDate(&cmd.startDate)
	w.numRetries(&cmd.numRetries, cmd.numRetries)
}

// Enabled is true if task is enabled.
func (cmd *TaskBase) Enabled() bool {
	return cmd.enabled
}

// Schedule creates schedule from flags.
func (cmd *TaskBase) Schedule() *managerclient.Schedule {
	return &managerclient.Schedule{
		Interval:   cmd.interval.String(),
		StartDate:  strfmt.DateTime(cmd.startDate.Time),
		NumRetries: int64(cmd.numRetries),
	}
}

// Update allows to differentiate instances created with NewUpdateTaskBase.
func (cmd *TaskBase) Update() bool {
	return cmd.update
}

// UpdateTask updates task fields if flags are set, returns true if there ware changes.
func (cmd *TaskBase) UpdateTask(task *managerclient.Task) bool {
	ok := false
	if cmd.Flag("enabled").Changed {
		task.Enabled = cmd.enabled
		ok = true
	}
	if cmd.Flag("interval").Changed {
		task.Schedule.Interval = cmd.interval.String()
		ok = true
	}
	if cmd.Flag("start-date").Changed {
		task.Schedule.StartDate = strfmt.DateTime(cmd.startDate.Time)
		ok = true
	}
	if cmd.Flag("num-retries").Changed {
		task.Schedule.NumRetries = int64(cmd.numRetries)
		ok = true
	}
	return ok
}
