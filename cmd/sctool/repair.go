// Copyright (C) 2017 ScyllaDB

package main

import (
	"context"
	"fmt"
	"io"
	"path"
	"strconv"
	"time"

	"github.com/pkg/errors"
	"github.com/scylladb/mermaid/mermaidclient"
	"github.com/spf13/cobra"
)

var repairCmd = withoutArgs(&cobra.Command{
	Use:   "repair",
	Short: "Manage repairs",
})

func init() {
	subcommand(repairCmd, rootCmd)
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
		activation, err := parseSchedStartDate(f.Value.String())
		if err != nil {
			return printableError{errors.Wrapf(err, "bad %q value: %s", f.Name, f.Value.String())}
		}
		t.Schedule.StartDate = activation.Format(time.RFC3339)

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

		id, err := client.CreateSchedTask(context.Background(), cfgCluster, t)
		if err != nil {
			return printableError{err}
		}

		fmt.Fprintln(cmd.OutOrStdout(), path.Join("repair", id))

		return nil
	},
}

func init() {
	cmd := repairSchedCmd
	subcommand(cmd, repairCmd)

	schedInitScheduleFlags(cmd)
}

var repairProgressCmd = &cobra.Command{
	Use:   "progress [repair/task-id]",
	Short: "Shows repair progress",
	Args:  cobra.MaximumNArgs(1),

	RunE: func(cmd *cobra.Command, args []string) error {
		var (
			repairTask string
			repairUnit string
			taskType   string
		)
		if len(args) > 0 {
			taskType, repairTask = path.Split(args[0])
		}
		if f := cmd.Flags().Lookup("unit"); f != nil {
			repairUnit = f.Value.String()
		}
		if repairUnit == "" && repairTask == "" {
			return printableError{errors.New("either task name/ID or repair unit name/ID must be specified")}
		}
		if repairUnit == "" {
			t, err := client.GetSchedTask(context.Background(), cfgCluster, taskType, repairTask)
			if err != nil {
				return printableError{err}
			}
			repairUnit = t.Properties["unit_id"]
		}

		status, progress, rows, err := client.RepairProgress(context.Background(), cfgCluster, repairUnit, repairTask)
		if err != nil {
			return printableError{err}
		}

		w := cmd.OutOrStdout()
		fmt.Fprintf(w, "Status: %s, progress: %d%%\n", status, progress)

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
	fmt.Fprint(w, t.Render())
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
	fmt.Fprint(w, t.Render())
}

func init() {
	cmd := repairProgressCmd
	subcommand(cmd, repairCmd)

	fs := cmd.Flags()
	fs.StringP("unit", "u", "", "repair unit `name` or ID")
	fs.Bool("details", false, "show detailed progress on shards")
}

var repairUnitCmd = withoutArgs(&cobra.Command{
	Use:   "unit",
	Short: "Manage repair units",
})

func init() {
	subcommand(repairUnitCmd, repairCmd)
}

var (
	cfgRepairUnitName     string
	cfgRepairUnitKeyspace string
	cfgRepairUnitTables   []string
)

func repairUnitInitCommonFlags(cmd *cobra.Command) {
	cmd.Flags().StringVarP(&cfgRepairUnitName, "name", "n", "", "alias `name`")
	cmd.Flags().StringVarP(&cfgRepairUnitKeyspace, "keyspace", "k", "", "keyspace `name`")
	cmd.Flags().StringSliceVarP(&cfgRepairUnitTables, "tables", "t", nil, "comma-separated `list` of tables in to repair in the keyspace, if empty repair the whole keyspace")
}

var repairUnitAddCmd = withoutArgs(&cobra.Command{
	Use:   "add",
	Short: "Creates a new repair unit",

	RunE: func(cmd *cobra.Command, args []string) error {
		id, err := client.CreateRepairUnit(context.Background(), cfgCluster, &mermaidclient.RepairUnit{
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
})

func init() {
	cmd := repairUnitAddCmd
	subcommand(cmd, repairUnitCmd)

	repairUnitInitCommonFlags(cmd)
	requireFlags(cmd, "keyspace")
}

var repairUnitUpdateCmd = &cobra.Command{
	Use:   "update <unit-id>",
	Short: "Modifies a repair unit",
	Args:  cobra.ExactArgs(1),

	RunE: func(cmd *cobra.Command, args []string) error {
		u, err := client.GetRepairUnit(context.Background(), cfgCluster, args[0])
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

		if err := client.UpdateRepairUnit(context.Background(), cfgCluster, u); err != nil {
			return printableError{err}
		}

		return nil
	},
}

func init() {
	cmd := repairUnitUpdateCmd
	subcommand(cmd, repairUnitCmd)

	repairUnitInitCommonFlags(cmd)
}

var repairUnitDeleteCmd = &cobra.Command{
	Use:   "delete <unit-id>",
	Short: "Deletes a repair unit",
	Args:  cobra.ExactArgs(1),

	RunE: func(cmd *cobra.Command, args []string) error {
		if err := client.DeleteRepairUnit(context.Background(), cfgCluster, args[0]); err != nil {
			return printableError{err}
		}

		return nil
	},
}

func init() {
	subcommand(repairUnitDeleteCmd, repairUnitCmd)
}

var repairUnitListCmd = withoutArgs(&cobra.Command{
	Use:   "list",
	Short: "Shows available repair units",

	RunE: func(cmd *cobra.Command, args []string) error {
		units, err := client.ListRepairUnits(context.Background(), cfgCluster)
		if err != nil {
			return printableError{err}
		}
		if len(units) == 0 {
			return nil
		}

		t := newTable("unit id", "name", "keyspace", "tables")
		for _, u := range units {
			t.AddRow(u.ID, u.Name, u.Keyspace, u.Tables)
		}
		fmt.Fprint(cmd.OutOrStdout(), t.Render())

		return nil
	},
})

func init() {
	subcommand(repairUnitListCmd, repairUnitCmd)
}
