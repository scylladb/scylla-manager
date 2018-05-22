// Copyright (C) 2017 ScyllaDB

package main

import (
	"fmt"
	"strconv"

	"github.com/pkg/errors"
	"github.com/scylladb/mermaid/mermaidclient"
	"github.com/spf13/cobra"
)

var repairCmd = &cobra.Command{
	Use:   "repair",
	Short: "Schedule repair",
	RunE: func(cmd *cobra.Command, args []string) error {
		t := &mermaidclient.Task{
			Type:    "repair",
			Enabled: true,
			Properties: map[string]string{
				"keyspace": cmd.Flag("keyspace").Value.String(),
				"tables":   cmd.Flag("tables").Value.String(),
			},
			Schedule: new(mermaidclient.Schedule),
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

		id, err := client.CreateTask(ctx, cfgCluster, t)
		if err != nil {
			return printableError{err}
		}

		fmt.Fprintln(cmd.OutOrStdout(), taskJoin("repair", id))

		return nil
	},
}

func init() {
	cmd := repairCmd
	register(repairCmd, rootCmd)

	taskInitCommonFlags(cmd)

	fs := cmd.Flags()
	fs.StringP("keyspace", "k", "", "keyspace `name`")
	fs.StringP("tables", "t", "", "comma-separated `list` of tables in to repair in the keyspace, if empty repair the whole keyspace")

	requireFlags(cmd, "keyspace")
}
