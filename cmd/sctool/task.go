// Copyright (C) 2017 ScyllaDB

package main

import (
	"fmt"
	"io"
	"strconv"

	"github.com/pkg/errors"
	"github.com/scylladb/mermaid/mermaidclient"
	"github.com/spf13/cobra"
)

func taskInitCommonFlags(cmd *cobra.Command) {
	fs := cmd.Flags()
	fs.StringP("start-date", "s", "now", "task start date in RFC3339 form or now[+duration]")
	fs.UintP("interval", "i", 7, "task schedule interval in `days`")
	fs.UintP("num-retries", "r", 3, "task schedule number of retries")
}

var taskCmd = &cobra.Command{
	Use:   "task",
	Short: "Manage tasks",
}

func init() {
	register(taskCmd, rootCmd)
}

var taskListCmd = &cobra.Command{
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

		var clusters []*mermaidclient.Cluster
		if cfgCluster == "" {
			clusters, err = client.ListClusters(ctx)
			if err != nil {
				return printableError{err}
			}
		} else {
			clusters = []*mermaidclient.Cluster{{ID: cfgCluster}}
		}

		w := cmd.OutOrStdout()
		for _, c := range clusters {
			// display cluster id if it's not specified.
			if cfgCluster == "" {
				fmt.Fprint(w, "cluster: ")
				if c.Name != "" {
					fmt.Fprintln(w, c.Name)
				} else {
					fmt.Fprintln(w, c.ID)
				}
			}

			tasks, err := client.ListSchedTasks(ctx, c.ID, taskType, all, status)
			if err != nil {
				return printableError{err}
			}
			printTasks(w, tasks, all)
		}

		return nil
	},
}

func printTasks(w io.Writer, tasks []*mermaidclient.ExtendedTask, all bool) {
	t := newTable("task", "start date", "int.", "ret.", "properties", "run start", "status")
	for _, task := range tasks {
		if !all && !task.Enabled {
			continue
		}

		fields := make([]interface{}, 0, 8)
		fields = append(fields, taskJoin(task.Type, task.ID))
		if task.Schedule != nil {
			fields = append(fields, formatTime(task.Schedule.StartDate), task.Schedule.IntervalDays, task.Schedule.NumRetries)
		} else {
			fields = append(fields, "-", "-", "-")
		}

		fields = append(fields, dumpMap(task.Properties))

		for _, f := range []string{
			formatTime(task.StartTime),
			task.Status,
		} {
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
	cmd := taskListCmd
	register(cmd, taskCmd)

	fs := cmd.Flags()
	fs.Bool("all", false, "list disabled tasks as well")
	fs.String("status", "", "filter tasks according to last run status")
	fs.StringP("type", "", "", "task type")
}

var taskStartCmd = &cobra.Command{
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
	register(taskStartCmd, taskCmd)
}

var taskStopCmd = &cobra.Command{
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
	register(taskStopCmd, taskCmd)
}

var taskHistoryCmd = &cobra.Command{
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
		t := newTable("id", "start time", "duration", "status", "cause")
		for _, r := range runs {
			fields := []interface{}{r.ID}
			for _, f := range []string{
				formatTime(r.StartTime),
				duration(r.StartTime, r.EndTime),
			} {
				if f == "" {
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
	cmd := taskHistoryCmd
	register(cmd, taskCmd)

	cmd.Flags().Int("limit", 10, "limit the number of returned results")
}

var taskUpdateCmd = &cobra.Command{
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
			startDate, err := parseStartDate(f.Value.String())
			if err != nil {
				return printableError{errors.Wrapf(err, "bad %q value: %s", f.Name, f.Value.String())}
			}
			t.Schedule.StartDate = startDate
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
	cmd := taskUpdateCmd
	register(cmd, taskCmd)

	taskInitCommonFlags(cmd)

	fs := cmd.Flags()
	fs.StringP("name", "n", "", "task name")
	fs.BoolP("enabled", "e", true, "enabled")
	fs.StringP("metadata", "m", "", "task metadata")
	fs.StringSlice("tags", nil, "tags")
}

var taskDeleteCmd = &cobra.Command{
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
	register(taskDeleteCmd, taskCmd)
}
