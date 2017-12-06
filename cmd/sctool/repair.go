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

var repairCmd = withoutArgs(&cobra.Command{
	Use:   "repair",
	Short: "Manage repairs",
})

func init() {
	subcommand(repairCmd, rootCmd)
}

func repairInitCommonFlags(cmd *cobra.Command) {
	cmd.Flags().StringVarP(&cfgRepairUnit, "unit", "u", "", "repair unit `name` or ID")
}

var repairProgressCmd = withoutArgs(&cobra.Command{
	Use:   "progress",
	Short: "Shows repair progress",

	RunE: func(cmd *cobra.Command, args []string) error {
		status, progress, rows, err := client.RepairProgress(context.Background(), cfgCluster, cfgRepairUnit, cfgRepairTask)
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
})

func init() {
	cmd := repairProgressCmd
	subcommand(cmd, repairCmd)

	repairInitCommonFlags(cmd)
	requireFlags(cmd, "unit")

	cmd.Flags().StringVarP(&cfgRepairTask, "task", "t", "", "repair task `ID`")
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
})

func init() {
	cmd := repairUnitAddCmd
	subcommand(cmd, repairUnitCmd)

	repairInitCommonFlags(cmd)
	repairUnitInitCommonFlags(cmd)
	requireFlags(cmd, "keyspace")
}

var repairUnitUpdateCmd = withoutArgs(&cobra.Command{
	Use:   "update",
	Short: "Modifies a repair unit",

	RunE: func(cmd *cobra.Command, args []string) error {
		u, err := client.GetRepairUnit(context.Background(), cfgCluster, cfgRepairUnit)
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

		if err := client.UpdateRepairUnit(context.Background(), u); err != nil {
			return printableError{err}
		}

		return nil
	},
})

func init() {
	cmd := repairUnitUpdateCmd
	subcommand(cmd, repairUnitCmd)

	repairInitCommonFlags(cmd)
	repairUnitInitCommonFlags(cmd)
	requireFlags(cmd, "unit")
}

var repairUnitDeleteCmd = withoutArgs(&cobra.Command{
	Use:   "delete",
	Short: "Deletes a repair unit",

	RunE: func(cmd *cobra.Command, args []string) error {
		if err := client.DeleteRepairUnit(context.Background(), cfgCluster, cfgRepairUnit); err != nil {
			return printableError{err}
		}

		return nil
	},
})

func init() {
	cmd := repairUnitDeleteCmd
	subcommand(cmd, repairUnitCmd)

	repairInitCommonFlags(cmd)
	requireFlags(cmd, "unit")
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
