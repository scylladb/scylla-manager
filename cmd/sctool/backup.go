// Copyright (C) 2017 ScyllaDB

package main

import (
	"fmt"
	"regexp"

	"github.com/pkg/errors"
	"github.com/scylladb/mermaid/internal/duration"
	"github.com/scylladb/mermaid/mermaidclient"
	"github.com/spf13/cobra"
)

var backupCmd = &cobra.Command{
	Use:   "backup",
	Short: "Schedule backup",
	RunE: func(cmd *cobra.Command, args []string) error {
		props := make(map[string]interface{})

		t := &mermaidclient.Task{
			Type:       "backup",
			Enabled:    true,
			Schedule:   new(mermaidclient.Schedule),
			Properties: props,
		}

		f := cmd.Flag("start-date")
		startDate, err := mermaidclient.ParseStartDate(f.Value.String())
		if err != nil {
			return printableError{err}
		}
		t.Schedule.StartDate = startDate

		i, err := cmd.Flags().GetString("interval")
		if err != nil {
			return printableError{err}
		}
		if _, err := duration.ParseDuration(i); err != nil {
			return printableError{err}
		}
		t.Schedule.Interval = i

		t.Schedule.NumRetries, err = cmd.Flags().GetInt64("num-retries")
		if err != nil {
			return printableError{err}
		}

		if f = cmd.Flag("keyspace"); f.Changed {
			keyspace, err := cmd.Flags().GetStringSlice("keyspace")
			if err != nil {
				return printableError{err}
			}
			props["keyspace"] = unescapeFilters(keyspace)
		}

		if f = cmd.Flag("dc"); f.Changed {
			dc, err := cmd.Flags().GetStringSlice("dc")
			if err != nil {
				return printableError{err}
			}
			props["dc"] = unescapeFilters(dc)
		}

		// Providers require that resource names are DNS compliant.
		// The following is a super simplified DNS (plus provider prefix)
		// matching regexp.
		providerDNS := regexp.MustCompile(`^(s3):([a-z0-9\-\.]+)$`)

		location := cmd.Flag("location").Value.String()
		if !providerDNS.MatchString(location) {
			return printableError{errors.New("invalid location")}
		}
		props["location"] = location

		if f = cmd.Flag("retention"); f.Changed {
			retention, err := duration.ParseDuration(f.Value.String())
			if err != nil {
				return printableError{err}
			}
			props["retention"] = retention
		}

		if f = cmd.Flag("rate-limit"); f.Changed {
			rateLimit, err := cmd.Flags().GetInt64("rate-limit")
			if err != nil {
				return printableError{err}
			}
			props["rate-limit"] = rateLimit
		}

		force, err := cmd.Flags().GetBool("force")
		if err != nil {
			return printableError{err}
		}

		id, err := client.CreateTask(ctx, cfgCluster, t, force)
		if err != nil {
			return printableError{err}
		}

		fmt.Fprintln(cmd.OutOrStdout(), mermaidclient.TaskJoin("backup", id))

		return nil
	},
}

func init() {
	cmd := backupCmd
	withScyllaDocs(cmd, "/sctool/#backup")
	register(cmd, rootCmd)

	fs := cmd.Flags()
	fs.StringSliceP("keyspace", "K", nil, "comma-separated `list` of keyspace/tables glob patterns, e.g. keyspace,!keyspace.table_prefix_*")
	fs.StringSlice("dc", nil, "comma-separated `list` of data centers glob patterns, e.g. dc1,!otherdc*")
	fs.StringP("location", "L", "", "where to save the backup, in a format <provider>:<path> ex. s3:my-bucket, the supported providers are: s3")
	fs.String("retention", "7d", "data retention, how long backup data shall be kept")
	fs.Int64("rate-limit", 0, "rate limit as megabytes (MiB) per second")
	fs.Bool("force", false, "force backup to skip database validation and schedule even if there are no matching keyspaces/tables")

	taskInitCommonFlags(fs)
	requireFlags(cmd, "location")
}
