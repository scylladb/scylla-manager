// Copyright (C) 2017 ScyllaDB

package main

import (
	"context"
	"fmt"

	"github.com/pkg/errors"
	"github.com/scylladb/mermaid/mermaidclient"
	"github.com/spf13/cobra"
)

var (
	cfgRepairUnit string
	cfgRepairTask string
)

var repairCmd = &cobra.Command{
	Use:   "repair",
	Short: "Manage repairs",
}

func init() {
	rootCmd.AddCommand(repairCmd)
}

func repairInitCommonFlags(cmd *cobra.Command) {
	cmd.Flags().StringVarP(&cfgRepairUnit, "unit", "u", "", "repair unit `name` or ID")

	cmd.MarkFlagRequired("unit")
}

var repairStartCmd = &cobra.Command{
	Use:   "start",
	Short: "Starts repair of a unit",

	RunE: func(cmd *cobra.Command, args []string) error {
		id, err := client.StartRepair(context.Background(), cfgRepairUnit)
		if err != nil {
			return printableError{err}
		}

		fmt.Fprintln(cmd.OutOrStdout(), id)

		return nil
	},
}

func init() {
	repairCmd.AddCommand(repairStartCmd)
	repairInitCommonFlags(repairStartCmd)
}

var repairStopCmd = &cobra.Command{
	Use:   "stop",
	Short: "Stops a running repair",

	RunE: func(cmd *cobra.Command, args []string) error {
		id, err := client.StopRepair(context.Background(), cfgRepairUnit)
		if err != nil {
			return printableError{err}
		}

		fmt.Fprintln(cmd.OutOrStdout(), id)

		return nil
	},
}

func init() {
	repairCmd.AddCommand(repairStopCmd)
	repairInitCommonFlags(repairStopCmd)
}

var repairProgressCmd = &cobra.Command{
	Use:   "progress",
	Short: "Shows repair progress",

	RunE: func(cmd *cobra.Command, args []string) error {
		status, progress, rows, err := client.RepairProgress(context.Background(), cfgRepairUnit, cfgRepairTask)
		if err != nil {
			return printableError{err}
		}

		w := cmd.OutOrStdout()
		fmt.Fprintf(w, "Status: %s, progress: %d%%\n", status, progress)

		t := newTable("host", "shard", "progress", "errors")
		for _, r := range rows {
			if r.Shard == -1 {
				t.append(r.Host, "-", "-", "-")
			} else {
				t.append(r.Host, r.Shard, r.Progress, r.Error)
			}
		}
		fmt.Fprint(w, t)

		return nil
	},
}

func init() {
	repairCmd.AddCommand(repairProgressCmd)
	repairInitCommonFlags(repairProgressCmd)
	repairProgressCmd.Flags().StringVarP(&cfgRepairTask, "task", "t", "", "repair task `ID`")

	repairProgressCmd.MarkFlagRequired("task")
}

var repairUnitCmd = &cobra.Command{
	Use:   "unit",
	Short: "Manage repair units",
}

func init() {
	repairCmd.AddCommand(repairUnitCmd)
}

var (
	cfgRepairUnitKeyspace string
	cfgRepairUnitTables   []string
)

func repairUnitInitCommonFlags(cmd *cobra.Command) {
	cmd.Flags().StringVarP(&cfgRepairUnitKeyspace, "keyspace", "k", "", "Keyspace `name`")
	cmd.Flags().StringSliceVarP(&cfgRepairUnitTables, "tables", "t", nil, "Comma-separated `list` of tables in to repair in the keyspace, if empty repair the whole keyspace")
}

var repairUnitCreateCmd = &cobra.Command{
	Use:   "create",
	Short: "Creates a new repair unit",

	RunE: func(cmd *cobra.Command, args []string) error {
		id, err := client.CreateRepairUnit(context.Background(), &mermaidclient.RepairUnit{
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
	repairUnitCmd.AddCommand(repairUnitCreateCmd)
	repairUnitInitCommonFlags(repairUnitCreateCmd)

	repairUnitCreateCmd.MarkFlagRequired("keyspace")
}

var repairUnitUpdateCmd = &cobra.Command{
	Use:   "update",
	Short: "Modifies existing repair unit",

	PreRunE: func(cmd *cobra.Command, args []string) error {
		ok := cmd.Flags().Changed("keyspace") || cmd.Flags().Changed("tables")
		if !ok {
			return errors.New("nothing to do")
		}

		return nil
	},

	RunE: func(cmd *cobra.Command, args []string) error {
		u, err := client.GetRepairUnit(context.Background(), cfgRepairUnit)
		if err != nil {
			return printableError{err}
		}

		if cmd.Flags().Changed("keyspace") {
			u.Keyspace = cfgRepairUnitKeyspace
		}
		if cmd.Flags().Changed("tables") {
			u.Tables = cfgRepairUnitTables
		}

		return client.UpdateRepairUnit(context.Background(), cfgRepairUnit, u)
	},
}

func init() {
	repairUnitCmd.AddCommand(repairUnitUpdateCmd)
	repairInitCommonFlags(repairUnitUpdateCmd)
	repairUnitInitCommonFlags(repairUnitUpdateCmd)
}

var repairUnitListCmd = &cobra.Command{
	Use:   "list",
	Short: "Shows repair units",

	RunE: func(cmd *cobra.Command, args []string) error {
		units, err := client.ListRepairUnits(context.Background())
		if err != nil {
			return printableError{err}
		}

		t := newTable("unit id", "keyspace", "tables")
		for _, u := range units {
			t.append(u.ID, u.Keyspace, u.Tables)
		}

		fmt.Fprint(cmd.OutOrStdout(), t)

		return nil
	},
}

func init() {
	repairUnitCmd.AddCommand(repairUnitListCmd)
}
