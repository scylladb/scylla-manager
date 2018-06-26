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

type tokenRangesKind string

const (
	pr  tokenRangesKind = "pr"
	npr tokenRangesKind = "npr"
	all tokenRangesKind = "all"
)

func (r tokenRangesKind) String() string {
	return string(r)
}

func (r *tokenRangesKind) Set(s string) error {
	switch tokenRangesKind(s) {
	case pr:
		*r = pr
	case npr:
		*r = npr
	case all:
		*r = all
	default:
		return errors.New("valid values are: pr, npr, all")
	}
	return nil
}

func (tokenRangesKind) Type() string {
	return "token ranges"
}

var repairTokenRanges = pr

var repairCmd = &cobra.Command{
	Use:   "repair",
	Short: "Schedule repair",
	RunE: func(cmd *cobra.Command, args []string) error {
		props := make(map[string]interface{})

		t := &mermaidclient.Task{
			Type:       "repair",
			Enabled:    true,
			Schedule:   new(mermaidclient.Schedule),
			Properties: props,
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
		props["filter"] = filter

		failFast, err := cmd.Flags().GetBool("fail-fast")
		if err != nil {
			return printableError{err}
		}
		if failFast {
			t.Schedule.NumRetries = 0
			props["fail_fast"] = true
		}

		props["token_ranges"] = repairTokenRanges.String()

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

	fs := cmd.Flags()
	fs.StringSliceP("filter", "F", nil, "comma-separated `list` of keyspace/tables glob patterns, i.e. keyspace,!keyspace.table_prefix_*")
	fs.Bool("fail-fast", false, "stop repair on first error")
	fs.Var(&repairTokenRanges, "token-ranges", "token ranges: pr - primary token ranges, npr - non primary token ranges, all - pr and npr")
	taskInitCommonFlags(cmd)
}
