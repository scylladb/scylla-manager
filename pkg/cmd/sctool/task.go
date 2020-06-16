// Copyright (C) 2017 ScyllaDB

package main

import (
	"fmt"
	"io"
	"sort"
	"strconv"
	"time"

	"github.com/go-openapi/strfmt"
	"github.com/pkg/errors"
	"github.com/scylladb/mermaid/pkg/mermaidclient"
	"github.com/scylladb/mermaid/pkg/service/scheduler"
	"github.com/scylladb/mermaid/pkg/util/duration"
	"github.com/scylladb/mermaid/pkg/util/timeutc"
	"github.com/scylladb/mermaid/pkg/util/uuid"
	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
)

func taskInitCommonFlags(fs *pflag.FlagSet) {
	fs.StringP("start-date", "s", "now", "specifies the task start date expressed in the RFC3339 format or now[+duration], e.g. now+3d2h10m, valid units are d, h, m, s")
	fs.StringP("interval", "i", "0", "task schedule interval e.g. 3d2h10m, valid units are d, h, m, s")
	fs.Int64P("num-retries", "r", 3, "the number of times a scheduled task will retry to run before failing")
}

var taskCmd = &cobra.Command{
	Use:   "task",
	Short: "Manages tasks",
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
			return err
		}
		status, err := fs.GetString("status")
		if err != nil {
			return err
		}
		taskType, err := fs.GetString("type")
		if err != nil {
			return err
		}
		sortKey, err := fs.GetString("sort")
		if err != nil {
			return err
		}

		if err := validateSortKey(sortKey); err != nil {
			return err
		}

		var clusters []*mermaidclient.Cluster
		if cfgCluster == "" {
			clusters, err = client.ListClusters(ctx)
			if err != nil {
				return err
			}
		} else {
			clusters = []*mermaidclient.Cluster{{ID: cfgCluster}}
		}

		w := cmd.OutOrStdout()
		for _, c := range clusters {
			// display cluster id if it's not specified.
			if cfgCluster == "" {
				mermaidclient.FormatClusterName(w, c)
			}
			tasks, err := client.ListTasks(ctx, c.ID, taskType, all, status)
			if err != nil {
				return err
			}

			sortTasks(tasks, taskListSortKey(sortKey))

			if err := render(w, tasks); err != nil {
				return err
			}
		}

		return nil
	},
}

type taskListSortKey string

const (
	taskListSortStartTime      taskListSortKey = "start-time"
	taskListSortEndTime        taskListSortKey = "end-time"
	taskListSortNextActivation taskListSortKey = "next-activation"
	taskListSortStatus         taskListSortKey = "status"
)

var allTaskSortKeys = []taskListSortKey{taskListSortStartTime, taskListSortNextActivation, taskListSortEndTime, taskListSortStatus}

var tasksSortFunctions = map[taskListSortKey]func(tasks mermaidclient.ExtendedTaskSlice){
	taskListSortStartTime:      sortTasksByStartTime,
	taskListSortEndTime:        sortTasksByEndTime,
	taskListSortNextActivation: sortTasksByNextActivation,
	taskListSortStatus:         sortTasksByStatus,
}

func validateSortKey(sortKey string) error {
	if sortKey == "" {
		return nil
	}

	for _, sk := range allTaskSortKeys {
		if string(sk) == sortKey {
			return nil
		}
	}
	return errors.Errorf("%s sort key not supported", sortKey)
}

func sortTasks(tasks mermaidclient.ExtendedTasks, key taskListSortKey) {
	if key == "" {
		return
	}
	tasksSortFunctions[key](tasks.ExtendedTaskSlice)
}

func timeLessFunc(lhvDate, rhvDate strfmt.DateTime) bool {
	lhv := timeutc.MustParse(time.RFC3339, lhvDate.String())
	rhv := timeutc.MustParse(time.RFC3339, rhvDate.String())
	return lhv.Before(rhv)
}

func sortTasksByNextActivation(tasks mermaidclient.ExtendedTaskSlice) {
	sort.Slice(tasks, func(i, j int) bool {
		return timeLessFunc(tasks[i].NextActivation, tasks[j].NextActivation)
	})
}

func sortTasksByStartTime(tasks mermaidclient.ExtendedTaskSlice) {
	sort.Slice(tasks, func(i, j int) bool {
		return timeLessFunc(tasks[i].StartTime, tasks[j].StartTime)
	})
}

func sortTasksByEndTime(tasks mermaidclient.ExtendedTaskSlice) {
	sort.Slice(tasks, func(i, j int) bool {
		return timeLessFunc(tasks[i].EndTime, tasks[j].EndTime)
	})
}

var taskStatusSortOrder = map[string]int{
	"NEW":     1,
	"RUNNING": 2,
	"STOPPED": 3,
	"DONE":    4,
	"ERROR":   5,
	"ABORTED": 6,
}

func sortTasksByStatus(tasks mermaidclient.ExtendedTaskSlice) {
	sort.Slice(tasks, func(i, j int) bool {
		return taskStatusSortOrder[tasks[i].Status] < taskStatusSortOrder[tasks[j].Status]
	})
}

func init() {
	cmd := taskListCmd
	withScyllaDocs(cmd, "/sctool/#task-list")
	register(cmd, taskCmd)

	fs := cmd.Flags()
	fs.BoolP("all", "a", false, "list disabled tasks as well")
	fs.StringP("status", "s", "", "filter tasks according to last run status")
	fs.StringP("type", "t", "", "task type")
	fs.String("sort", "", fmt.Sprintf("returned results will be sorted by given key, valid values: %s", allTaskSortKeys))
}

var taskStartCmd = &cobra.Command{
	Use:   "start <type/task-id>",
	Short: "Starts executing a task",
	Args:  cobra.ExactArgs(1),

	RunE: func(cmd *cobra.Command, args []string) error {
		taskType, taskID, err := mermaidclient.TaskSplit(args[0])
		if err != nil {
			return err
		}

		cont := true
		if f := cmd.Flag("no-continue"); f.Changed {
			cont = false
		}

		if err := client.StartTask(ctx, cfgCluster, taskType, taskID, cont); err != nil {
			return err
		}
		return nil
	},
}

func init() {
	cmd := taskStartCmd
	withScyllaDocs(cmd, "/sctool/#task-start")
	register(cmd, taskCmd)

	fs := cmd.Flags()
	fs.Bool("no-continue", false, "do not resume last run")
}

var taskStopCmd = &cobra.Command{
	Use:   "stop <type/task-id>",
	Short: "Stops the currently running task instance",
	Args:  cobra.ExactArgs(1),

	RunE: func(cmd *cobra.Command, args []string) error {
		taskType, taskID, err := mermaidclient.TaskSplit(args[0])
		if err != nil {
			return err
		}

		disable, err := cmd.Flags().GetBool("disable")
		if err != nil {
			return err
		}

		if err := client.StopTask(ctx, cfgCluster, taskType, taskID, disable); err != nil {
			return err
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
	Short: "Shows run history of a task",
	Args:  cobra.ExactArgs(1),

	RunE: func(cmd *cobra.Command, args []string) error {
		taskType, taskID, err := mermaidclient.TaskSplit(args[0])
		if err != nil {
			return err
		}

		limit, err := cmd.Flags().GetInt64("limit")
		if err != nil {
			return err
		}

		runs, err := client.GetTaskHistory(ctx, cfgCluster, taskType, taskID, limit)
		if err != nil {
			return err
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
	Short: "Modifies a task",
	Args:  cobra.ExactArgs(1),

	RunE: func(cmd *cobra.Command, args []string) error {
		taskType, taskID, err := mermaidclient.TaskSplit(args[0])
		if err != nil {
			return err
		}

		t, err := client.GetTask(ctx, cfgCluster, taskType, taskID)
		if err != nil {
			return err
		}

		changed := false
		if f := cmd.Flag("enabled"); f.Changed {
			t.Enabled, err = strconv.ParseBool(f.Value.String())
			if err != nil {
				return err
			}
			changed = true
		}
		if f := cmd.Flag("start-date"); f.Changed {
			startDate, err := mermaidclient.ParseStartDate(f.Value.String())
			if err != nil {
				return err
			}
			t.Schedule.StartDate = startDate
			changed = true
		}
		if f := cmd.Flag("interval"); f.Changed {
			i, err := cmd.Flags().GetString("interval")
			if err != nil {
				return err
			}
			if _, err := duration.ParseDuration(i); err != nil {
				return err
			}
			t.Schedule.Interval = i
			changed = true
		}
		if f := cmd.Flag("num-retries"); f.Changed {
			t.Schedule.NumRetries, err = cmd.Flags().GetInt64("num-retries")
			if err != nil {
				return err
			}
			changed = true
		}
		if !changed {
			return errors.New("nothing to change")
		}

		if err := client.UpdateTask(ctx, cfgCluster, taskType, taskID, t); err != nil {
			return err
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
	Short: "Deletes a task",
	Args:  cobra.ExactArgs(1),

	RunE: func(cmd *cobra.Command, args []string) error {
		taskType, taskID, err := mermaidclient.TaskSplit(args[0])
		if err != nil {
			return err
		}

		if err := client.DeleteTask(ctx, cfgCluster, taskType, taskID); err != nil {
			return err
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
	Short: "Shows a task progress",
	Args:  cobra.ExactArgs(1),

	RunE: func(cmd *cobra.Command, args []string) error {
		w := cmd.OutOrStdout()

		taskType, taskID, err := mermaidclient.TaskSplit(args[0])
		if err != nil {
			return err
		}

		t, err := client.GetTask(ctx, cfgCluster, taskType, taskID)
		if err != nil {
			return err
		}

		runID, err := cmd.Flags().GetString("run")
		if err != nil {
			return err
		}
		if runID != "" {
			if _, err = uuid.Parse(runID); err != nil {
				return err
			}
		} else {
			runID = "latest"
		}

		switch scheduler.TaskType(taskType) {
		case scheduler.HealthCheckAlternatorTask, scheduler.HealthCheckCQLTask, scheduler.HealthCheckRESTTask:
			fmt.Fprintf(w, "Use: sctool status -c %s\n", cfgCluster)
			return statusCmd.RunE(statusCmd, nil)
		case scheduler.RepairTask:
			return renderRepairProgress(cmd, w, t, runID)
		case scheduler.BackupTask:
			return renderBackupProgress(cmd, w, t, runID)
		}

		return nil
	},
}

func renderRepairProgress(cmd *cobra.Command, w io.Writer, t *mermaidclient.Task, runID string) error {
	rp, err := client.RepairProgress(ctx, cfgCluster, t.ID, runID)
	if err != nil {
		return err
	}

	rp.Detailed, err = cmd.Flags().GetBool("details")
	if err != nil {
		return err
	}

	hf, err := cmd.Flags().GetStringSlice("host")
	if err != nil {
		return err
	}
	if err := rp.SetHostFilter(hf); err != nil {
		return err
	}

	kf, err := cmd.Flags().GetStringSlice("keyspace")
	if err != nil {
		return err
	}
	if err := rp.SetKeyspaceFilter(kf); err != nil {
		return err
	}

	rp.Task = t

	return render(w, rp)
}

func renderBackupProgress(cmd *cobra.Command, w io.Writer, t *mermaidclient.Task, runID string) error {
	rp, err := client.BackupProgress(ctx, cfgCluster, t.ID, runID)
	if err != nil {
		return err
	}

	rp.Detailed, err = cmd.Flags().GetBool("details")
	if err != nil {
		return err
	}

	hf, err := cmd.Flags().GetStringSlice("host")
	if err != nil {
		return err
	}
	if err := rp.SetHostFilter(hf); err != nil {
		return err
	}

	kf, err := cmd.Flags().GetStringSlice("keyspace")
	if err != nil {
		return err
	}
	if err := rp.SetKeyspaceFilter(kf); err != nil {
		return err
	}

	rp.Task = t
	rp.AggregateErrors()

	return render(w, rp)
}

func init() {
	cmd := taskProgressCmd
	withScyllaDocs(cmd, "/sctool/#task-progress")
	register(cmd, taskCmd)

	fs := cmd.Flags()
	fs.Bool("details", false, "show detailed progress")
	fs.StringSliceP("keyspace", "K", nil, "a comma-separated `list` of keyspace/tables glob patterns, e.g. 'keyspace,!keyspace.table_prefix_*'")
	fs.StringSlice("host", nil, "a comma-separated list of host glob patterns, e.g. '1.1.1.*,!1.2.*.4")
	fs.String("run", "", "show progress of a particular run, see sctool task history")
}
