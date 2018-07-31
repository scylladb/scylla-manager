// Copyright (C) 2017 ScyllaDB

package main

import (
	"fmt"
	"time"

	"github.com/scylladb/mermaid/mermaidclient"
	"github.com/spf13/cobra"
)

var healthcheckCmd = &cobra.Command{
	Use:   "healthcheck",
	Short: "Schedule a health check",
	RunE: func(cmd *cobra.Command, args []string) error {
		t := &mermaidclient.Task{
			Type:       "healthcheck",
			Enabled:    true,
			Schedule:   new(mermaidclient.Schedule),
			Properties: make(map[string]interface{}),
		}

		startDate, err := parseStartDate("now")
		if err != nil {
			return printableError{err}
		}
		t.Schedule.StartDate = startDate

		d, err := cmd.Flags().GetDuration("interval")
		if err != nil {
			return printableError{err}
		}
		t.Schedule.Interval = d.String()

		t.Schedule.NumRetries = 0

		id, err := client.CreateTask(ctx, cfgCluster, t)
		if err != nil {
			return printableError{err}
		}

		fmt.Fprintln(cmd.OutOrStdout(), taskJoin("healthcheck", id))

		return nil
	},
}

func init() {
	cmd := healthcheckCmd
	register(healthcheckCmd, rootCmd)
	fs := cmd.Flags()
	fs.DurationP("interval", "i", 15*time.Second, "check interval")
}
