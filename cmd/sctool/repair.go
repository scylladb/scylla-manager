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

var repairCmd = &cobra.Command{
	Use:   "repair",
	Short: "Manage repairs",
}

func init() {
	register(repairCmd, rootCmd)
}

var repairSchedCmd = &cobra.Command{
	Use:   "schedule <unit-id>",
	Short: "Schedule repair of a unit",
	Args:  cobra.ExactArgs(1),

	RunE: func(cmd *cobra.Command, args []string) error {
		t := &mermaidclient.Task{
			Type:       "repair",
			Enabled:    true,
			Properties: map[string]string{"unit_id": args[0]},
			Schedule:   new(mermaidclient.Schedule),
		}

		f := cmd.Flag("start-date")
		startDate, err := parseStartDate(f.Value.String())
		if err != nil {
			return printableError{errors.Wrapf(err, "bad %q value: %s", f.Name, f.Value.String())}
		}
		t.Schedule.StartDate = startDate

		f = cmd.Flag("interval")
		interval, err := strconv.Atoi(f.Value.String())
		if err != nil {
			return printableError{errors.Wrapf(err, "bad %q value: %s", f.Name, f.Value.String())}
		}

		t.Schedule.IntervalDays = int32(interval)
		f = cmd.Flag("num-retries")
		numRetries, err := strconv.Atoi(f.Value.String())
		if err != nil {
			return printableError{errors.Wrapf(err, "bad %q value: %s", f.Name, f.Value.String())}
		}
		t.Schedule.NumRetries = int32(numRetries)

		id, err := client.CreateSchedTask(ctx, cfgCluster, t)
		if err != nil {
			return printableError{err}
		}

		fmt.Fprintln(cmd.OutOrStdout(), taskJoin("repair", id))

		return nil
	},
}

func init() {
	cmd := repairSchedCmd
	register(cmd, repairCmd)

	taskInitCommonFlags(cmd)
}

var repairProgressCmd = &cobra.Command{
	Use:   "progress <repair/task-id>",
	Short: "Shows repair progress",
	Args:  cobra.ExactArgs(1),

	RunE: func(cmd *cobra.Command, args []string) error {
		w := cmd.OutOrStdout()

		taskType, taskID, err := taskSplit(args[0])
		if err != nil {
			return printableError{err}
		}
		if taskType == "" {
			taskType = "repair"
		}
		if taskType != "repair" {
			return printableError{errors.Errorf("unexpected task type %q", taskType)}
		}

		t, err := client.GetSchedTask(ctx, cfgCluster, taskType, taskID)
		if err != nil {
			return printableError{err}
		}
		unitID := t.Properties["unit_id"]

		hist, err := client.GetSchedTaskHistory(ctx, cfgCluster, taskType, taskID, 1)
		if err != nil {
			return printableError{err}
		}
		if len(hist) == 0 {
			fmt.Fprintf(w, "Task did not run yet\n")
			return nil
		}
		run := hist[0]

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

		if run.Status == runner.StatusError.String() {
			return nil
		}

		_, _, progress, rows, err := client.RepairProgress(ctx, cfgCluster, unitID, run.ID)
		if err != nil {
			return printableError{err}
		}
		fmt.Fprintf(w, "Progress:\t%d%%\n", progress)

		details, err := cmd.Flags().GetBool("details")
		if err != nil {
			return printableError{err}
		}

		if details {
			printDetailedProgress(w, rows)
			return nil
		}

		printHostOnlyProgress(w, rows)

		return nil
	},
}

func printHostOnlyProgress(w io.Writer, rows []mermaidclient.RepairProgressRow) {
	t := newTable("host", "progress", "failed segments")
	for _, r := range rows {
		// ignore shard details when host only requested
		if r.Shard != -1 {
			continue
		}
		if r.Empty {
			t.AddRow(r.Host, "-", "-")
		} else {
			t.AddRow(r.Host, r.Progress, r.Error)
		}
	}
	fmt.Fprint(w, t)
}

func printDetailedProgress(w io.Writer, rows []mermaidclient.RepairProgressRow) {
	t := newTable("host", "shard", "progress", "failed segments")
	for _, r := range rows {
		// ignore host-only entries when details requested
		if r.Shard == -1 {
			continue
		}
		if r.Empty {
			t.AddRow(r.Host, "-", "-", "-")
		} else {
			t.AddRow(r.Host, r.Shard, r.Progress, r.Error)
		}
	}
	fmt.Fprint(w, t)
}

func init() {
	cmd := repairProgressCmd
	register(cmd, repairCmd)

	fs := cmd.Flags()
	fs.Bool("details", false, "show detailed progress on shards")
}

var repairUnitCmd = &cobra.Command{
	Use:   "unit",
	Short: "Manage repair units",
}

func init() {
	register(repairUnitCmd, repairCmd)
}

var (
	cfgRepairUnitName     string
	cfgRepairUnitKeyspace string
	cfgRepairUnitTables   []string
)

func repairUnitInitCommonFlags(cmd *cobra.Command) {
	fs := cmd.Flags()
	fs.StringVarP(&cfgRepairUnitName, "name", "n", "", "alias `name`")
	fs.StringVarP(&cfgRepairUnitKeyspace, "keyspace", "k", "", "keyspace `name`")
	fs.StringSliceVarP(&cfgRepairUnitTables, "tables", "t", nil, "comma-separated `list` of tables in to repair in the keyspace, if empty repair the whole keyspace")
}

var repairUnitAddCmd = &cobra.Command{
	Use:   "add",
	Short: "Creates a new repair unit",

	RunE: func(cmd *cobra.Command, args []string) error {
		id, err := client.CreateRepairUnit(ctx, cfgCluster, &mermaidclient.RepairUnit{
			Name:     cfgRepairUnitName,
			Keyspace: cfgRepairUnitKeyspace,
			Tables:   cfgRepairUnitTables,
		})
		if err != nil {
			return printableError{err}
		}

		fmt.Fprintln(cmd.OutOrStdout(), id)

		return nil
	},
}

func init() {
	cmd := repairUnitAddCmd
	register(cmd, repairUnitCmd)

	repairUnitInitCommonFlags(cmd)
	requireFlags(cmd, "keyspace")
}

var repairUnitUpdateCmd = &cobra.Command{
	Use:   "update <unit-id>",
	Short: "Modifies a repair unit",
	Args:  cobra.ExactArgs(1),

	RunE: func(cmd *cobra.Command, args []string) error {
		u, err := client.GetRepairUnit(ctx, cfgCluster, args[0])
		if err != nil {
			return printableError{err}
		}

		ok := false
		if cmd.Flags().Changed("name") {
			u.Name = cfgRepairUnitName
			ok = true
		}
		if cmd.Flags().Changed("keyspace") {
			u.Keyspace = cfgRepairUnitKeyspace
			ok = true
		}
		if cmd.Flags().Changed("tables") {
			u.Tables = cfgRepairUnitTables
			ok = true
		}
		if !ok {
			return errors.New("nothing to do")
		}

		if err := client.UpdateRepairUnit(ctx, cfgCluster, u); err != nil {
			return printableError{err}
		}

		return nil
	},
}

func init() {
	cmd := repairUnitUpdateCmd
	register(cmd, repairUnitCmd)

	repairUnitInitCommonFlags(cmd)
}

var repairUnitDeleteCmd = &cobra.Command{
	Use:   "delete <unit-id>",
	Short: "Deletes a repair unit",
	Args:  cobra.ExactArgs(1),

	RunE: func(cmd *cobra.Command, args []string) error {
		if err := client.DeleteRepairUnit(ctx, cfgCluster, args[0]); err != nil {
			return printableError{err}
		}

		return nil
	},
}

func init() {
	register(repairUnitDeleteCmd, repairUnitCmd)
}

var repairUnitListCmd = &cobra.Command{
	Use:   "list",
	Short: "Shows available repair units",

	RunE: func(cmd *cobra.Command, args []string) error {
		units, err := client.ListRepairUnits(ctx, cfgCluster)
		if err != nil {
			return printableError{err}
		}

		t := newTable("unit id", "name", "keyspace", "tables")
		for _, u := range units {
			t.AddRow(u.ID, u.Name, u.Keyspace, u.Tables)
		}
		fmt.Fprint(cmd.OutOrStdout(), t)

		return nil
	},
}

func init() {
	register(repairUnitListCmd, repairUnitCmd)
}
