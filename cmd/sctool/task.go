// Copyright (C) 2017 ScyllaDB

package main

import (
	"fmt"
	"io"
	"strconv"

	"github.com/pkg/errors"
	"github.com/scylladb/mermaid/mermaidclient"
	"github.com/scylladb/mermaid/sched/runner"
	"github.com/spf13/cobra"
)

func taskInitCommonFlags(cmd *cobra.Command) {
	fs := cmd.Flags()
	fs.StringP("start-date", "s", "now", "task start date in RFC3339 form or now[+duration]")
	fs.UintP("interval", "i", 0, "task schedule interval in `days`")
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

			tasks, err := client.ListTasks(ctx, c.ID, taskType, all, status)
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

		if props, ok := task.Properties.(map[string]interface{}); ok {
			fields = append(fields, dumpMap(props))
		}

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

		if err := client.StartTask(ctx, cfgCluster, taskType, taskID); err != nil {
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
		if err := client.StopTask(ctx, cfgCluster, taskType, taskID); err != nil {
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

		limit, err := cmd.Flags().GetInt64("limit")
		if err != nil {
			return printableError{err}
		}

		runs, err := client.GetTaskHistory(ctx, cfgCluster, taskType, taskID, limit)
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

		t, err := client.GetTask(ctx, cfgCluster, taskType, taskID)
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
			t.Schedule.IntervalDays = int64(interval)
			changed = true
		}
		if f := cmd.Flag("num-retries"); f.Changed {
			numRetries, err := strconv.Atoi(f.Value.String())
			if err != nil {
				return printableError{errors.Wrapf(err, "bad %q value: %s", f.Name, f.Value.String())}
			}
			t.Schedule.NumRetries = int64(numRetries)
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

		if err := client.DeleteTask(ctx, cfgCluster, taskType, taskID); err != nil {
			return printableError{err}
		}
		return nil
	},
}

func init() {
	register(taskDeleteCmd, taskCmd)
}

var taskProgressCmd = &cobra.Command{
	Use:   "progress <type/task-id>",
	Short: "Shows task progress",
	Args:  cobra.ExactArgs(1),

	RunE: func(cmd *cobra.Command, args []string) error {
		w := cmd.OutOrStdout()

		taskType, taskID, err := taskSplit(args[0])
		if err != nil {
			return printableError{err}
		}
		if taskType != "repair" {
			return printableError{errors.Errorf("unexpected task type %q", taskType)}
		}

		t, err := client.GetTask(ctx, cfgCluster, taskType, taskID)
		if err != nil {
			return printableError{err}
		}

		hist, err := client.GetTaskHistory(ctx, cfgCluster, taskType, taskID, 1)
		if err != nil {
			return printableError{err}
		}
		if len(hist) == 0 {
			fmt.Fprintf(w, "Task did not run yet\n")
			return nil
		}
		run := hist[0]

		printProgressHeader(w, run)

		if run.Status == runner.StatusError.String() {
			return nil
		}

		prog, err := client.RepairProgress(ctx, cfgCluster, t.ID, run.ID)
		if err != nil {
			return printableError{err}
		}

		printRepairUnitProgress(w, prog)

		details, err := cmd.Flags().GetBool("details")
		if err != nil {
			return printableError{err}
		}
		if details {
			printRepairUnitDetailedProgress(w, prog)
		}
		return nil
	},
}

func printProgressHeader(w io.Writer, run *mermaidclient.TaskRun) {
	fmt.Fprintf(w, "Status:\t\t%s", run.Status)
	if run.Cause != "" {
		fmt.Fprintf(w, " (%s)", run.Cause)
	}
	fmt.Fprintln(w)
	fmt.Fprintf(w, "Start time:\t%s\n", formatTime(run.StartTime))
	if !isZero(run.EndTime) {
		fmt.Fprintf(w, "End time:\t%s\n", formatTime(run.EndTime))
	}
	fmt.Fprintf(w, "Duration:\t%s\n", duration(run.StartTime, run.EndTime))
	fmt.Fprintln(w)
}

func printRepairUnitProgress(w io.Writer, prog *mermaidclient.RepairProgress) {
	t := newTable("keyspace", "progress")
	t.AddRow("total", fmt.Sprintf("%d%%", prog.PercentComplete))
	for _, u := range prog.Units {
		t.AddRow(u.Unit.Keyspace, fmt.Sprintf("%d%%", u.PercentComplete))
	}
	fmt.Fprintln(w, t)
}

func printRepairUnitDetailedProgress(w io.Writer, prog *mermaidclient.RepairProgress) {
	for _, u := range prog.Units {
		fmt.Fprintln(w, u.Unit.Keyspace)
		t := newTable("host", "shard", "progress", "segment_count", "segment_success", "segment_error")
		for _, n := range u.Nodes {
			for i, s := range n.Shards {
				t.AddRow(n.Host, i, fmt.Sprintf("%d%%", u.PercentComplete), s.SegmentCount, s.SegmentSuccess, s.SegmentError)
			}
		}
		fmt.Fprintln(w, t)
	}
}

func init() {
	cmd := taskProgressCmd
	register(cmd, taskCmd)

	fs := cmd.Flags()
	fs.Bool("details", false, "show detailed progress")
}
