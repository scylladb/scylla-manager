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

		locations, err := cmd.Flags().GetStringSlice("location")
		if err != nil {
			return printableError{err}
		}
		if err := validateLocations(locations); err != nil {
			return printableError{err}
		}
		props["location"] = locations

		if f = cmd.Flag("retention"); f.Changed {
			retention, err := cmd.Flags().GetInt("retention")
			if err != nil {
				return printableError{err}
			}
			props["retention"] = retention
		}

		if f = cmd.Flag("rate-limit"); f.Changed {
			rateLimits, err := cmd.Flags().GetStringSlice("rate-limit")
			if err != nil {
				return printableError{err}
			}
			if err := validateRateLimits(rateLimits); err != nil {
				return printableError{err}
			}
			props["rate_limit"] = rateLimits
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

func validateLocations(locations []string) error {
	// Providers require that resource names are DNS compliant.
	// The following is a super simplified DNS (plus provider prefix)
	// matching regexp.
	providerDNSPattern := regexp.MustCompile(`^([a-z0-9\-\.]+:)?(s3):([a-z0-9\-\.]+)$`)

	for _, l := range locations {
		if !providerDNSPattern.MatchString(l) {
			return errors.Errorf("invalid location %s", l)
		}
	}
	return nil
}

func validateRateLimits(rateLimits []string) error {
	rateLimitPattern := regexp.MustCompile(`^([a-z0-9\-\.]+:)([0-9]+)$`)

	for _, r := range rateLimits {
		if !rateLimitPattern.MatchString(r) {
			return printableError{errors.Errorf("invalid rate-limit %s", r)}
		}
	}
	return nil
}

func init() {
	cmd := backupCmd
	withScyllaDocs(cmd, "/sctool/#backup")
	register(cmd, rootCmd)

	fs := cmd.Flags()
	fs.StringSliceP("keyspace", "K", nil, "comma-separated `list` of keyspace/tables glob patterns, e.g. keyspace,!keyspace.table_prefix_*")
	fs.StringSlice("dc", nil, "comma-separated `list` of data centers glob patterns, e.g. dc1,!otherdc*")
	fs.StringSliceP("location", "L", nil, "comma-separated `list` of backup locations in the format <dc>:<provider>:<path>, the dc part is optional and only needed when different datacenters upload data to different locations, the supported providers are: s3") //nolint: lll
	fs.Int("retention", 3, "data retention, how many backups shall be kept")
	fs.StringSlice("rate-limit", nil, "comma-separated `list` of rate limit as megabytes (MiB) per second in the format <dc>:<limit>, the dc part is optional and only needed when different datacenters need different upload limits") //nolint: lll
	fs.Bool("force", false, "force backup to skip database validation and schedule even if there are no matching keyspaces/tables")

	taskInitCommonFlags(fs)
	requireFlags(cmd, "location")
}
