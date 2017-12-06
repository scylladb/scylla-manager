// Copyright (C) 2017 ScyllaDB

package main

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/pkg/errors"
	"github.com/spf13/cobra"
)

var taskCmd = &cobra.Command{
	Use:   "task",
	Short: "Manage tasks",
}

func init() {
	subcommand(taskCmd, rootCmd)
}

var (
	schedTaskID   string
	schedTaskType string
)

var schedTaskListCmd = &cobra.Command{
	Use:   "list",
	Short: "Shows available tasks",

	RunE: func(cmd *cobra.Command, args []string) error {
		tasks, err := client.ListSchedTasks(context.Background(), cfgCluster, schedTaskType)
		if err != nil {
			return printableError{err}
		}
		if len(tasks) == 0 {
			return nil
		}

		t := newTable("task id", "name", "type", "start date", "interval days", "num retries")
		for _, task := range tasks {
			if task.Schedule != nil {
				t.AddRow(task.ID, task.Name, task.Type, task.Schedule.StartDate, task.Schedule.IntervalDays, task.Schedule.NumRetries)
			} else {
				t.AddRow(task.ID, task.Name, task.Type, "-", "-", "-")
			}
		}
		fmt.Fprint(cmd.OutOrStdout(), t.Render())

		return nil
	},
}

func init() {
	subcommand(schedTaskListCmd, taskCmd)

	schedTaskListCmd.Flags().StringVarP(&schedTaskType, "type", "", "", "task type")
}

func schedTaskInitCommonFlags(cmd *cobra.Command) {
	cmd.Flags().StringVarP(&schedTaskType, "type", "", "", "task type")
	cmd.Flags().StringVarP(&schedTaskID, "task", "t", "", "task `name` or ID")

	requireFlags(cmd, "type", "task")
}

var schedStartTaskCmd = &cobra.Command{
	Use:   "start",
	Short: "Starts executing a task",

	RunE: func(cmd *cobra.Command, args []string) error {
		if err := client.SchedStartTask(context.Background(), cfgCluster, schedTaskType, schedTaskID); err != nil {
			return printableError{err}
		}
		return nil
	},
}

func init() {
	subcommand(schedStartTaskCmd, taskCmd)

	schedTaskInitCommonFlags(schedStartTaskCmd)
}

var schedStopTaskCmd = &cobra.Command{
	Use:   "stop",
	Short: "Stops executing a task",

	RunE: func(cmd *cobra.Command, args []string) error {
		if err := client.SchedStopTask(context.Background(), cfgCluster, schedTaskType, schedTaskID); err != nil {
			return printableError{err}
		}
		return nil
	},
}

func init() {
	subcommand(schedStopTaskCmd, taskCmd)

	schedTaskInitCommonFlags(schedStopTaskCmd)
}

func schedInitTaskPayloadFlags(cmd *cobra.Command) {
	fs := cmd.Flags()
	fs.StringP("name", "n", "", "task name")
	fs.BoolP("enabled", "e", true, "enabled")
	fs.StringP("metadata", "m", "", "task metadata")
	fs.StringSlice("tags", nil, "tags")
}

func schedInitScheduleFlags(cmd *cobra.Command) {
	fs := cmd.Flags()
	fs.StringP("start-date", "s", "now", "task start date in RFC3339 form or now[+duration]")
	fs.UintP("interval", "i", 7, "task schedule interval in `days`")
	fs.UintP("num-retries", "r", 3, "task schedule number of retries")
}

func parseSchedStartDate(startDate string) (time.Time, error) {
	const nowSafety = 30 * time.Second

	if strings.HasPrefix(startDate, "now") {
		now := time.Now()
		var d time.Duration
		if startDate != "now" {
			var err error
			d, err = time.ParseDuration(startDate[3:])
			if err != nil {
				return time.Time{}, err
			}
		}

		activation := now.Add(d)
		if activation.Before(now.Add(nowSafety)) {
			activation = now.Add(nowSafety)
		}
		return activation.UTC(), nil
	}

	t, err := time.Parse(time.RFC3339, startDate)
	if err != nil {
		return time.Time{}, err
	}
	return t.UTC(), nil
}

var schedTaskUpdateCmd = withoutArgs(&cobra.Command{
	Use:   "update",
	Short: "Modifies a task",

	RunE: func(cmd *cobra.Command, args []string) error {
		t, err := client.GetSchedTask(context.Background(), cfgCluster, schedTaskType, schedTaskID)
		if err != nil {
			return printableError{err}
		}

		changed := false
		if f := cmd.Flag("name"); f.Changed {
			t.Name = f.Value.String()
			changed = true
		}
		if f := cmd.Flag("enabled"); f.Changed {
			var err error
			t.Enabled, err = strconv.ParseBool(f.Value.String())
			if err != nil {
				return printableError{errors.Wrapf(err, "bad %q value: %s", f.Name, f.Value.String())}
			}
			changed = true
		}
		if f := cmd.Flag("metadata"); f.Changed {
			t.Metadata = f.Value.String()
			changed = true
		}
		if f := cmd.Flag("tags"); f.Changed {
			var err error
			t.Tags, err = cmd.Flags().GetStringSlice("tags")
			if err != nil {
				return printableError{errors.Wrapf(err, "bad %q value: %s", f.Name, f.Value.String())}
			}
			changed = true
		}
		if f := cmd.Flag("start-date"); f.Changed {
			startDate, err := parseSchedStartDate(f.Value.String())
			if err != nil {
				return printableError{errors.Wrapf(err, "bad %q value: %s", f.Name, f.Value.String())}
			}
			t.Schedule.StartDate = startDate.Format(time.RFC3339)
			changed = true
		}
		if f := cmd.Flag("interval"); f.Changed {
			interval, err := strconv.Atoi(f.Value.String())
			if err != nil {
				return printableError{errors.Wrapf(err, "bad %q value: %s", f.Name, f.Value.String())}
			}
			t.Schedule.IntervalDays = int32(interval)
			changed = true
		}
		if f := cmd.Flag("num-retries"); f.Changed {
			numRetries, err := strconv.Atoi(f.Value.String())
			if err != nil {
				return printableError{errors.Wrapf(err, "bad %q value: %s", f.Name, f.Value.String())}
			}
			t.Schedule.NumRetries = int32(numRetries)
			changed = true
		}
		if !changed {
			return errors.New("nothing to change")
		}

		if err := client.UpdateTask(context.Background(), cfgCluster, schedTaskType, schedTaskID, t); err != nil {
			return printableError{err}
		}

		return nil
	},
})

func init() {
	cmd := schedTaskUpdateCmd
	subcommand(cmd, taskCmd)

	schedTaskInitCommonFlags(cmd)
	schedInitTaskPayloadFlags(cmd)
	schedInitScheduleFlags(cmd)
}

var schedDeleteTaskCmd = &cobra.Command{
	Use:   "delete",
	Short: "Deletes a task schedule",

	RunE: func(cmd *cobra.Command, args []string) error {
		if err := client.SchedDeleteTask(context.Background(), cfgCluster, schedTaskType, schedTaskID); err != nil {
			return printableError{err}
		}
		return nil
	},
}

func init() {
	subcommand(schedDeleteTaskCmd, taskCmd)

	schedTaskInitCommonFlags(schedDeleteTaskCmd)
}
