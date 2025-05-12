// Copyright (C) 2015 ScyllaDB

package scheduler

import (
	"context"
	"encoding/json"

	stderr "errors"

	"github.com/pkg/errors"
	"github.com/scylladb/scylla-manager/v3/pkg/util/uuid"
)

const (
	disabledByLabel = "disabled-by"
	disabledByValue = "stopping-runner"
)

// StoppingRunner wraps Runner and before executing it will fetch all tasks disable and stop them.
// Once wrapped Runner execution is finished all disabled tasks will be re-enabled.
type StoppingRunner struct {
	Runner  Runner
	Service *Service
	// TaskType of a task this runner is executing.
	TaskType TaskType
}

type taskOptions struct {
	StopAll bool `json:"stop_all"`
}

// Run implements Runner interface.
func (sr StoppingRunner) Run(ctx context.Context, clusterID, taskID, runID uuid.UUID, properties json.RawMessage) (err error) {
	var opt taskOptions
	if err := json.Unmarshal(properties, &opt); err != nil {
		return errors.Wrap(err, "unmarshal task properties")
	}
	defer func() {
		if dErr := sr.enableAllTasks(ctx, clusterID, opt.StopAll); dErr != nil {
			err = stderr.Join(err, errors.Wrap(dErr, "enable all tasks"))
		}
	}()
	if err := sr.disableAndStopAllTasks(ctx, clusterID, opt.StopAll); err != nil {
		return errors.Wrap(err, "disable and stop all tasks")
	}
	return sr.Runner.Run(ctx, clusterID, taskID, runID, properties)
}

func (sr StoppingRunner) disableAndStopAllTasks(ctx context.Context, clusterID uuid.UUID, stopAll bool) error {
	if !stopAll {
		return nil
	}
	tasks, err := sr.Service.ListTasks(ctx, clusterID, ListFilter{})
	if err != nil {
		return errors.Wrap(err, "list tasks")
	}
	for _, task := range tasks {
		// Prevent from stopping itself or tasks of the same type.
		if task.Type == sr.TaskType {
			sr.Service.logger.Debug(ctx, "Ignoring task", "task", task.Task)
			continue
		}

		// Mark all disabled tasks with special label, so we can easily find them and re-enable.
		if task.Labels == nil {
			task.Labels = map[string]string{}
		}
		task.Labels[disabledByLabel] = disabledByValue
		if task.Enabled {
			task.Enabled = false
		}
		if err := sr.Service.PutTask(ctx, &task.Task); err != nil {
			return errors.Wrap(err, "disable task")
		}
		if err := sr.Service.StopTask(ctx, &task.Task); err != nil {
			return errors.Wrap(err, "stop task")
		}
	}
	return nil
}

func (sr StoppingRunner) enableAllTasks(ctx context.Context, clusterID uuid.UUID, stopAll bool) error {
	if !stopAll {
		return nil
	}
	tasks, err := sr.Service.ListTasks(ctx, clusterID, ListFilter{
		Disabled: true,
	})
	if err != nil {
		return errors.Wrap(err, "list tasks")
	}

	for _, task := range tasks {
		if task.Enabled {
			continue
		}
		if task.Type == sr.TaskType {
			continue
		}
		// Enabling only tasks that were disabled by StoppingRunner.
		if val := task.Labels[disabledByLabel]; val != disabledByValue {
			continue
		}
		task.Enabled = true
		delete(task.Labels, disabledByLabel)
		if err := sr.Service.PutTask(ctx, &task.Task); err != nil {
			return errors.Wrap(err, "enable task")
		}
	}
	return nil
}
