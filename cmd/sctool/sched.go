// Copyright (C) 2017 ScyllaDB

package main

import (
	"fmt"
	"io"
	"strconv"
	"strings"
	"time"

	"github.com/pkg/errors"
	"github.com/scylladb/mermaid/mermaidclient"
	"github.com/spf13/cobra"
)

var taskCmd = &cobra.Command{
	Use:   "task",
	Short: "Manage tasks",
}

func init() {
	subcommand(taskCmd, rootCmd)
}

var schedTaskListCmd = withoutArgs(&cobra.Command{
	Use:   "list",
	Short: "Shows available tasks and their last run status",

	RunE: func(cmd *cobra.Command, args []string) error {
		fs := cmd.Flags()
		all, err := fs.GetBool("all")
		if err != nil {
			return printableError{err}
		}
		status, err := fs.GetString("status")
		if err != nil {
			return printableError{err}
		}
		taskType, err := fs.GetString("type")
		if err != nil {
			return printableError{err}
		}

		tasks, err := client.ListSchedTasks(ctx, cfgCluster, taskType, all, status)
		if err != nil {
			return printableError{err}
		}

		printTasks(cmd.OutOrStdout(), tasks, all)

		return nil
	},
})

func printTasks(w io.Writer, tasks []*mermaidclient.ExtendedTask, all bool) {
	t := newTable("task", "start date", "int.", "ret.", "properties", "run start", "run stop", "status")
	for _, task := range tasks {
		if !all && !task.Enabled {
			continue
		}

		fields := make([]interface{}, 0, 8)
		fields = append(fields, taskJoin(task.Type, task.ID))
		if task.Schedule != nil {
			fields = append(fields, task.Schedule.StartDate, task.Schedule.IntervalDays, task.Schedule.NumRetries)
		} else {
			fields = append(fields, "-", "-", "-")
		}

		fields = append(fields, dumpMap(task.Properties))

		for _, f := range []string{task.StartTime, task.EndTime, task.Status} {
			if f == "" {
				f = "-"
			}
			fields = append(fields, f)
		}
		t.AddRow(fields...)
	}
	fmt.Fprint(w, t)
}

func init() {
	cmd := schedTaskListCmd
	subcommand(cmd, taskCmd)

	fs := cmd.Flags()
	fs.Bool("all", false, "list disabled tasks as well")
	fs.String("status", "", "filter tasks according to last run status")
	fs.StringP("type", "", "", "task type")
}

var schedStartTaskCmd = &cobra.Command{
	Use:   "start <type/task-id>",
	Short: "Starts executing a task",
	Args:  cobra.ExactArgs(1),

	RunE: func(cmd *cobra.Command, args []string) error {
		taskType, taskID, err := taskSplit(args[0])
		if err != nil {
			return printableError{err}
		}

		if err := client.SchedStartTask(ctx, cfgCluster, taskType, taskID); err != nil {
			return printableError{err}
		}
		return nil
	},
}

func init() {
	subcommand(schedStartTaskCmd, taskCmd)
}

var schedStopTaskCmd = &cobra.Command{
	Use:   "stop <type/task-id>",
	Short: "Stops executing a task",
	Args:  cobra.ExactArgs(1),

	RunE: func(cmd *cobra.Command, args []string) error {
		taskType, taskID, err := taskSplit(args[0])
		if err != nil {
			return printableError{err}
		}
		if err := client.SchedStopTask(ctx, cfgCluster, taskType, taskID); err != nil {
			return printableError{err}
		}
		return nil
	},
}

func init() {
	subcommand(schedStopTaskCmd, taskCmd)
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

var schedTaskUpdateCmd = &cobra.Command{
	Use:   "update <type/task-id>",
	Short: "Modifies a task",
	Args:  cobra.ExactArgs(1),

	RunE: func(cmd *cobra.Command, args []string) error {
		taskType, taskID, err := taskSplit(args[0])
		if err != nil {
			return printableError{err}
		}

		t, err := client.GetSchedTask(ctx, cfgCluster, taskType, taskID)
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

		if err := client.UpdateTask(ctx, cfgCluster, taskType, taskID, t); err != nil {
			return printableError{err}
		}

		return nil
	},
}

func init() {
	cmd := schedTaskUpdateCmd
	subcommand(cmd, taskCmd)

	schedInitTaskPayloadFlags(cmd)
	schedInitScheduleFlags(cmd)
}

var schedDeleteTaskCmd = &cobra.Command{
	Use:   "delete <type/task-id>",
	Short: "Deletes a task schedule",
	Args:  cobra.ExactArgs(1),

	RunE: func(cmd *cobra.Command, args []string) error {
		taskType, taskID, err := taskSplit(args[0])
		if err != nil {
			return printableError{err}
		}

		if err := client.SchedDeleteTask(ctx, cfgCluster, taskType, taskID); err != nil {
			return printableError{err}
		}
		return nil
	},
}

func init() {
	subcommand(schedDeleteTaskCmd, taskCmd)
}

var schedTaskHistoryCmd = &cobra.Command{
	Use:   "history <type/task-id>",
	Short: "list run history of a task",
	Args:  cobra.ExactArgs(1),

	RunE: func(cmd *cobra.Command, args []string) error {
		taskType, taskID, err := taskSplit(args[0])
		if err != nil {
			return printableError{err}
		}

		limit, err := cmd.Flags().GetInt("limit")
		if err != nil {
			return printableError{err}
		}

		runs, err := client.GetSchedTaskHistory(ctx, cfgCluster, taskType, taskID, limit)
		if err != nil {
			return printableError{err}
		}
		t := newTable("id", "start time", "stop time", "status", "cause")
		for _, r := range runs {
			fields := []interface{}{r.ID}
			for _, f := range []string{r.StartTime, r.EndTime} {
				t, err := time.Parse(time.RFC3339, f)
				if err != nil {
					return printableError{err}
				}
				if t.IsZero() {
					f = "-"
				}
				fields = append(fields, f)
			}
			cause := "-"
			if r.Status == "error" {
				cause = r.Cause
			}
			fields = append(fields, r.Status, cause)
			t.AddRow(fields...)
		}
		fmt.Fprint(cmd.OutOrStdout(), t)

		return nil
	},
}

func init() {
	cmd := schedTaskHistoryCmd
	subcommand(cmd, taskCmd)

	cmd.Flags().Int("limit", 10, "limit the number of returned results")
}
