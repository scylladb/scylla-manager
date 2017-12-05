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
	initClusterFlag(repairCmd, repairCmd.PersistentFlags())
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
				t.AddRow(r.Host, "-", "-", "-")
			} else {
				t.AddRow(r.Host, r.Shard, r.Progress, r.Error)
			}
		}
		fmt.Fprint(w, t.Render())

		return nil
	},
}

func init() {
	repairCmd.AddCommand(repairProgressCmd)
	repairInitCommonFlags(repairProgressCmd)
	repairProgressCmd.Flags().StringVarP(&cfgRepairTask, "task", "t", "", "repair task `ID`")
}

var repairUnitCmd = &cobra.Command{
	Use:   "unit",
	Short: "Manage repair units",
}

func init() {
	repairCmd.AddCommand(repairUnitCmd)
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

var repairUnitAddCmd = &cobra.Command{
	Use:   "add",
	Short: "Creates a new repair unit",

	RunE: func(cmd *cobra.Command, args []string) error {
		id, err := client.CreateRepairUnit(context.Background(), &mermaidclient.RepairUnit{
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
	repairUnitCmd.AddCommand(repairUnitAddCmd)
	repairUnitInitCommonFlags(repairUnitAddCmd)

	repairUnitAddCmd.MarkFlagRequired("keyspace")
}

var repairUnitUpdateCmd = &cobra.Command{
	Use:   "update",
	Short: "Modifies a repair unit",

	RunE: func(cmd *cobra.Command, args []string) error {
		u, err := client.GetRepairUnit(context.Background(), cfgRepairUnit)
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

		if err := client.UpdateRepairUnit(context.Background(), cfgRepairUnit, u); err != nil {
			return printableError{err}
		}

		return nil
	},
}

func init() {
	repairUnitCmd.AddCommand(repairUnitUpdateCmd)
	repairInitCommonFlags(repairUnitUpdateCmd)
	repairUnitInitCommonFlags(repairUnitUpdateCmd)
}

var repairUnitDeleteCmd = &cobra.Command{
	Use:   "delete",
	Short: "Deletes a repair unit",

	RunE: func(cmd *cobra.Command, args []string) error {
		if err := client.DeleteRepairUnit(context.Background(), cfgRepairUnit); err != nil {
			return printableError{err}
		}

		return nil
	},
}

func init() {
	repairUnitCmd.AddCommand(repairUnitDeleteCmd)
	repairInitCommonFlags(repairUnitDeleteCmd)
}

var repairUnitListCmd = &cobra.Command{
	Use:   "list",
	Short: "Shows available repair units",

	RunE: func(cmd *cobra.Command, args []string) error {
		units, err := client.ListRepairUnits(context.Background())
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
}

func init() {
	repairUnitCmd.AddCommand(repairUnitListCmd)
}
