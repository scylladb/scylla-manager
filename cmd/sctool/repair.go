// Copyright (C) 2017 ScyllaDB

package main

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/pkg/errors"
	"github.com/scylladb/mermaid/mermaidclient"
	"github.com/spf13/cobra"
)

var repairCmd = &cobra.Command{
	Use:   "repair",
	Short: "Schedule repair",
	RunE: func(cmd *cobra.Command, args []string) error {
		t := &mermaidclient.Task{
			Type:     "repair",
			Enabled:  true,
			Schedule: new(mermaidclient.Schedule),
		}

		f := cmd.Flag("start-date")
		startDate, err := parseStartDate(f.Value.String())
		if err != nil {
			return printableError{errors.Wrapf(err, "bad %q value: %s", f.Name, f.Value.String())}
		}
		t.Schedule.StartDate = startDate

		f = cmd.Flag("interval-days")
		intervalDays, err := strconv.Atoi(f.Value.String())
		if err != nil {
			return printableError{errors.Wrapf(err, "bad %q value: %s", f.Name, f.Value.String())}
		}
		t.Schedule.IntervalDays = int64(intervalDays)

		f = cmd.Flag("num-retries")
		numRetries, err := strconv.Atoi(f.Value.String())
		if err != nil {
			return printableError{errors.Wrapf(err, "bad %q value: %s", f.Name, f.Value.String())}
		}
		t.Schedule.NumRetries = int64(numRetries)

		filter, err := cmd.Flags().GetStringSlice("filter")
		if err != nil {
			return printableError{err}
		}
		// accommodate for escaping of bash expansions, we can safely remove '\'
		// as it's not a valid char in keyspace or table name
		for i := range filter {
			filter[i] = strings.Replace(filter[i], "\\", "", -1)
		}

		props := make(map[string]interface{})
		props["filter"] = filter
		t.Properties = props

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

	cmd.Flags().StringSliceP("filter", "F", nil, "comma-separated `list` of keyspace/tables glob patterns, i.e. keyspace,!keyspace.table_prefix_*")
	requireFlags(cmd, "filter")
	taskInitCommonFlags(cmd)
}
