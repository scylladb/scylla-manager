// Copyright (C) 2017 ScyllaDB

package main

import (
	"fmt"
	"strconv"

	"github.com/pkg/errors"
	"github.com/scylladb/mermaid/internal/duration"
	"github.com/scylladb/mermaid/mermaidclient"
	"github.com/scylladb/mermaid/uuid"
	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
)

func taskInitCommonFlags(fs *pflag.FlagSet) {
	fs.StringP("start-date", "s", "now", "task start date in RFC3339 form or now[+duration], e.g. now+3d2h10m, valid units are d, h, m, s")
	fs.StringP("interval", "i", "0", "task schedule interval e.g. 3d2h10m, valid units are d, h, m, s")
	fs.Int64P("num-retries", "r", 3, "task schedule number of retries")
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
	Short: "Show available tasks and their last run status",

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
				fmt.Fprint(w, "Cluster: ")
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

			if err := render(w, tasks); err != nil {
				return printableError{err}
			}
		}

		return nil
	},
}

func init() {
	cmd := taskListCmd
	withScyllaDocs(cmd, "/sctool/#task-list")
	register(cmd, taskCmd)

	fs := cmd.Flags()
	fs.BoolP("all", "a", false, "list disabled tasks as well")
	fs.StringP("status", "s", "", "filter tasks according to last run status")
	fs.StringP("type", "t", "", "task type")
}

var taskStartCmd = &cobra.Command{
	Use:   "start <type/task-id>",
	Short: "Start executing a task",
	Args:  cobra.ExactArgs(1),

	RunE: func(cmd *cobra.Command, args []string) error {
		taskType, taskID, err := mermaidclient.TaskSplit(args[0])
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
	withScyllaDocs(cmd, "/sctool/#task-start")
	register(cmd, taskCmd)

	fs := cmd.Flags()
	fs.Bool("continue", true, "try resuming last run")
}

var taskStopCmd = &cobra.Command{
	Use:   "stop <type/task-id>",
	Short: "Stop the currently running task instance",
	Args:  cobra.ExactArgs(1),

	RunE: func(cmd *cobra.Command, args []string) error {
		taskType, taskID, err := mermaidclient.TaskSplit(args[0])
		if err != nil {
			return printableError{err}
		}

		disable, err := cmd.Flags().GetBool("disable")
		if err != nil {
			return printableError{err}
		}

		if err := client.StopTask(ctx, cfgCluster, taskType, taskID, disable); err != nil {
			return printableError{err}
		}
		return nil
	},
}

func init() {
	cmd := taskStopCmd
	withScyllaDocs(cmd, "/sctool/#task-stop")
	register(cmd, taskCmd)

	fs := cmd.Flags()
	fs.Bool("disable", false, "do not run in future")
}

var taskHistoryCmd = &cobra.Command{
	Use:   "history <type/task-id>",
	Short: "Show run history of a task",
	Args:  cobra.ExactArgs(1),

	RunE: func(cmd *cobra.Command, args []string) error {
		taskType, taskID, err := mermaidclient.TaskSplit(args[0])
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

		return render(cmd.OutOrStdout(), runs)
	},
}

func init() {
	cmd := taskHistoryCmd
	withScyllaDocs(cmd, "/sctool/#task-history")
	register(cmd, taskCmd)

	cmd.Flags().Int64("limit", 10, "limit the number of returned results")
}

var taskUpdateCmd = &cobra.Command{
	Use:   "update <type/task-id>",
	Short: "Modify a task",
	Args:  cobra.ExactArgs(1),

	RunE: func(cmd *cobra.Command, args []string) error {
		taskType, taskID, err := mermaidclient.TaskSplit(args[0])
		if err != nil {
			return printableError{err}
		}

		t, err := client.GetTask(ctx, cfgCluster, taskType, taskID)
		if err != nil {
			return printableError{err}
		}

		changed := false
		if f := cmd.Flag("enabled"); f.Changed {
			t.Enabled, err = strconv.ParseBool(f.Value.String())
			if err != nil {
				return printableError{err}
			}
			changed = true
		}
		if f := cmd.Flag("start-date"); f.Changed {
			startDate, err := mermaidclient.ParseStartDate(f.Value.String())
			if err != nil {
				return printableError{err}
			}
			t.Schedule.StartDate = startDate
			changed = true
		}
		if f := cmd.Flag("interval"); f.Changed {
			i, err := cmd.Flags().GetString("interval")
			if err != nil {
				return printableError{err}
			}
			if _, err := duration.ParseDuration(i); err != nil {
				return printableError{err}
			}
			t.Schedule.Interval = i
			changed = true
		}
		if f := cmd.Flag("num-retries"); f.Changed {
			t.Schedule.NumRetries, err = cmd.Flags().GetInt64("num-retries")
			if err != nil {
				return printableError{err}
			}
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
	withScyllaDocs(cmd, "/sctool/#task-update")
	register(cmd, taskCmd)

	fs := cmd.Flags()
	fs.StringP("enabled", "e", "true", "enabled")
	taskInitCommonFlags(fs)
}

var taskDeleteCmd = &cobra.Command{
	Use:   "delete <type/task-id>",
	Short: "Delete a task",
	Args:  cobra.ExactArgs(1),

	RunE: func(cmd *cobra.Command, args []string) error {
		taskType, taskID, err := mermaidclient.TaskSplit(args[0])
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
	cmd := taskDeleteCmd
	withScyllaDocs(cmd, "/sctool/#task-delete")
	register(cmd, taskCmd)
}

var taskProgressCmd = &cobra.Command{
	Use:   "progress <type/task-id>",
	Short: "Show a task progress",
	Args:  cobra.ExactArgs(1),

	RunE: func(cmd *cobra.Command, args []string) error {
		w := cmd.OutOrStdout()

		taskType, taskID, err := mermaidclient.TaskSplit(args[0])
		if err != nil {
			return printableError{err}
		}

		if taskType == "healthcheck" {
			w.Write([]byte("Use: sctool status -c " + cfgCluster + "\n")) // nolint: errcheck
			return statusCmd.RunE(statusCmd, nil)
		}

		t, err := client.GetTask(ctx, cfgCluster, taskType, taskID)
		if err != nil {
			return printableError{err}
		}

		runID, err := cmd.Flags().GetString("run")
		if err != nil {
			return printableError{err}
		}
		if runID != "" {
			if _, err = uuid.Parse(runID); err != nil {
				return printableError{err}
			}
		} else {
			runID = "latest"
		}

		rp, err := client.RepairProgress(ctx, cfgCluster, t.ID, runID)
		if err != nil {
			return printableError{err}
		}

		rp.Detailed, err = cmd.Flags().GetBool("details")
		if err != nil {
			return printableError{err}
		}

		hf, err := cmd.Flags().GetStringSlice("host")
		if err != nil {
			return printableError{err}
		}
		if err := rp.SetHostFilter(hf); err != nil {
			return printableError{err}
		}

		kf, err := cmd.Flags().GetStringSlice("keyspace")
		if err != nil {
			return printableError{err}
		}
		if err := rp.SetKeyspaceFilter(kf); err != nil {
			return printableError{err}
		}

		rp.Task = t

		return render(w, rp)
	},
}

func init() {
	cmd := taskProgressCmd
	withScyllaDocs(cmd, "/sctool/#task-progress")
	register(cmd, taskCmd)

	fs := cmd.Flags()
	fs.Bool("details", false, "show detailed progress")
	fs.StringSliceP("keyspace", "K", nil, "comma-separated `list` of keyspace glob patterns to filter progress details by")
	fs.StringSlice("host", nil, "comma-separated `list` of host glob patterns to filter progress details by")
	fs.String("run", "", "show progress of a particular run, see sctool task history")
}
