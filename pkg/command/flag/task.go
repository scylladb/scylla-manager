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
	name       string
	interval   Duration
	startDate  Time
	numRetries int
}

func MakeTaskBase() TaskBase {
	return TaskBase{}
}

func NewUpdateTaskBase() TaskBase {
	return TaskBase{
		Command: cobra.Command{
			Args: cobra.MaximumNArgs(1),
		},
		update: true,
	}
}

func (cmd *TaskBase) Init() {
	w := Wrap(cmd.Flags())
	w.enabled(&cmd.enabled)
	w.name(&cmd.name)
	w.interval(&cmd.interval)
	w.startDate(&cmd.startDate)
	w.numRetries(&cmd.numRetries, cmd.numRetries)
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
			Interval:   cmd.interval.String(),
			StartDate:  strfmt.DateTime(cmd.startDate.Value()),
			NumRetries: int64(cmd.numRetries),
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
	if cmd.Flag("interval").Changed {
		task.Schedule.Interval = cmd.interval.String()
		ok = true
	}
	if cmd.Flag("start-date").Changed {
		task.Schedule.StartDate = strfmt.DateTime(cmd.startDate.Value())
		ok = true
	}
	if cmd.Flag("num-retries").Changed {
		task.Schedule.NumRetries = int64(cmd.numRetries)
		ok = true
	}
	return ok
}
