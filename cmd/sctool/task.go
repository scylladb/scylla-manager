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
	fs.UintP("interval-days", "i", 0, "task schedule interval in `days`")
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
	p := newTable("task", "next run", "ret.", "properties", "status")
	for _, t := range tasks {
		id := taskJoin(t.Type, t.ID)
		if all && !t.Enabled {
			id = "*" + id
		}
		r := formatTime(t.NextActivation)
		if t.Schedule.IntervalDays != 0 {
			r += fmt.Sprint(" (+", t.Schedule.IntervalDays, " days)")
		}
		s := t.Status
		if t.Cause != "" {
			s += " " + t.Cause
		}
		p.AddRow(id, r, t.Schedule.NumRetries, dumpMap(t.Properties.(map[string]interface{})), s)
	}
	fmt.Fprint(w, p)
}

func init() {
	cmd := taskListCmd
	register(cmd, taskCmd)

	fs := cmd.Flags()
	fs.BoolP("all", "a", false, "list disabled tasks as well")
	fs.StringP("status", "s", "", "filter tasks according to last run status")
	fs.StringP("type", "t", "", "task type")
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

		cont, err := cmd.Flags().GetBool("continue")
		if err != nil {
			return printableError{err}
		}

		if err := client.StartTask(ctx, cfgCluster, taskType, taskID, cont); err != nil {
			return printableError{err}
		}
		return nil
	},
}

func init() {
	cmd := taskStartCmd
	register(cmd, taskCmd)

	fs := cmd.Flags()
	fs.Bool("continue", true, "try resuming last run")
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

		t := newTable("id", "start time", "end time", "duration", "status")
		for _, r := range runs {
			s := r.Status
			if r.Cause != "" {
				s += " " + r.Cause
			}
			t.AddRow(r.ID, formatTime(r.StartTime), formatTime(r.EndTime), formatDuration(r.StartTime, r.EndTime), s)
		}
		fmt.Fprint(cmd.OutOrStdout(), t)

		return nil
	},
}

func init() {
	cmd := taskHistoryCmd
	register(cmd, taskCmd)

	cmd.Flags().Int64("limit", 10, "limit the number of returned results")
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
		if f := cmd.Flag("interval-days"); f.Changed {
			intervalDays, err := strconv.Atoi(f.Value.String())
			if err != nil {
				return printableError{errors.Wrapf(err, "bad %q value: %s", f.Name, f.Value.String())}
			}
			t.Schedule.IntervalDays = int64(intervalDays)
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

		p := newTable()
		addProgressHeader(p, run)

		prog, err := client.RepairProgress(ctx, cfgCluster, t.ID, run.ID)
		if err != nil {
			return printableError{err}
		}
		if prog.PercentComplete == 0 {
			if _, err := w.Write([]byte(p.String())); err != nil {
				return printableError{err}
			}
			return nil
		}

		addRepairProgressHeader(p, prog)
		p.AddSeparator()
		addRepairUnitProgress(p, prog)
		if _, err := w.Write([]byte(p.String())); err != nil {
			return printableError{err}
		}

		details, err := cmd.Flags().GetBool("details")
		if err != nil {
			return printableError{err}
		}
		if details {
			d := newTable()
			for i, u := range prog.Units {
				if i > 0 {
					d.AddSeparator()
				}
				d.AddRow(u.Unit.Keyspace, "shard", "progress", "segment_count", "segment_success", "segment_error")
				if len(u.Nodes) > 0 {
					d.AddSeparator()
					addRepairUnitDetailedProgress(d, u)
				}
			}
			if _, err := w.Write([]byte(d.String())); err != nil {
				return printableError{err}
			}
		}

		return nil
	},
}

func addProgressHeader(t *table, run *mermaidclient.TaskRun) {
	t.AddRow("Status", run.Status)
	if run.Cause != "" {
		t.AddRow("Cause", run.Cause)
	}
	t.AddRow("Start time", formatTime(run.StartTime))
	if !isZero(run.EndTime) {
		t.AddRow("End time", formatTime(run.EndTime))
	}
	t.AddRow("Duration", formatDuration(run.StartTime, run.EndTime))
}

func addRepairProgressHeader(t *table, prog *mermaidclient.RepairProgress) {
	t.AddRow("Progress", formatPercent(prog.PercentComplete))
	if len(prog.Dcs) > 0 {
		t.AddRow("Datacenters", prog.Dcs)
	}
	if prog.Ranges != "" {
		t.AddRow("Token ranges", prog.Ranges)
	}
}

func addRepairUnitProgress(t *table, prog *mermaidclient.RepairProgress) {
	for _, u := range prog.Units {
		var se int64
		for _, n := range u.Nodes {
			for _, s := range n.Shards {
				se += s.SegmentError
			}
		}
		p := formatPercent(u.PercentComplete)
		if se > 0 {
			p += " (has errors)"
		}
		t.AddRow(u.Unit.Keyspace, p)
	}
}

func addRepairUnitDetailedProgress(t *table, u *mermaidclient.RepairUnitProgress) {
	for _, n := range u.Nodes {
		for i, s := range n.Shards {
			t.AddRow(n.Host, i, fmt.Sprintf("%d%%", s.PercentComplete), s.SegmentCount, s.SegmentSuccess, s.SegmentError)
		}
	}
}

func init() {
	cmd := taskProgressCmd
	register(cmd, taskCmd)

	fs := cmd.Flags()
	fs.Bool("details", false, "show detailed progress")
}
