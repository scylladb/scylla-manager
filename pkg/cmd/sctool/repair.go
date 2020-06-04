// Copyright (C) 2017 ScyllaDB

package main

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/pkg/errors"
	"github.com/scylladb/mermaid/pkg/mermaidclient"
	"github.com/scylladb/mermaid/pkg/util/duration"
	"github.com/spf13/cobra"
)

var repairCmd = &cobra.Command{
	Use:   "repair",
	Short: "Schedules repair",
	RunE: func(cmd *cobra.Command, args []string) error {
		props := make(map[string]interface{})

		t := &mermaidclient.Task{
			Type:       "repair",
			Enabled:    true,
			Schedule:   new(mermaidclient.Schedule),
			Properties: props,
		}

		f := cmd.Flag("start-date")
		startDate, err := mermaidclient.ParseStartDate(f.Value.String())
		if err != nil {
			return err
		}
		t.Schedule.StartDate = startDate

		i, err := cmd.Flags().GetString("interval")
		if err != nil {
			return err
		}
		if _, err := duration.ParseDuration(i); err != nil {
			return err
		}
		t.Schedule.Interval = i

		t.Schedule.NumRetries, err = cmd.Flags().GetInt64("num-retries")
		if err != nil {
			return err
		}

		if f = cmd.Flag("keyspace"); f.Changed {
			keyspace, err := cmd.Flags().GetStringSlice("keyspace")
			if err != nil {
				return err
			}
			props["keyspace"] = unescapeFilters(keyspace)
		}

		if f = cmd.Flag("dc"); f.Changed {
			dc, err := cmd.Flags().GetStringSlice("dc")
			if err != nil {
				return err
			}
			props["dc"] = unescapeFilters(dc)
		}

		failFast, err := cmd.Flags().GetBool("fail-fast")
		if err != nil {
			return err
		}
		if failFast {
			t.Schedule.NumRetries = 0
			props["fail_fast"] = true
		}

		force, err := cmd.Flags().GetBool("force")
		if err != nil {
			return err
		}

		dryRun, err := cmd.Flags().GetBool("dry-run")
		if err != nil {
			return err
		}

		if f = cmd.Flag("intensity"); f.Changed {
			intensity, err := cmd.Flags().GetFloat64("intensity")
			if err != nil {
				return err
			}
			props["intensity"] = intensity
		}

		if dryRun {
			res, err := client.GetRepairTarget(ctx, cfgCluster, t)
			if err != nil {
				return err
			}
			showTables, err := cmd.Flags().GetBool("show-tables")
			if err != nil {
				return err
			}
			if showTables {
				res.ShowTables = -1
			}

			fmt.Fprintf(cmd.OutOrStderr(), "NOTICE: dry run mode, repair is not scheduled\n\n")
			return res.Render(cmd.OutOrStdout())
		}

		id, err := client.CreateTask(ctx, cfgCluster, t, force)
		if err != nil {
			return err
		}

		fmt.Fprintln(cmd.OutOrStdout(), mermaidclient.TaskJoin("repair", id))

		return nil
	},
}

func init() {
	cmd := repairCmd
	withScyllaDocs(cmd, "/sctool/#repair")
	register(cmd, rootCmd)

	fs := cmd.Flags()
	fs.StringSliceP("keyspace", "K", nil,
		"a comma-separated `list` of keyspace/tables glob patterns, e.g. 'keyspace,!keyspace.table_prefix_*' used to include or exclude keyspaces from backup")
	fs.StringSlice("dc", nil, "a comma-separated `list` of datacenter glob patterns, e.g. 'dc1,!otherdc*', used to specify the DCs to include or exclude from repair")
	fs.Bool("fail-fast", false, "stop repair on first error")
	fs.Bool("force", false, "force repair to skip database validation and schedule even if there are no matching keyspaces/tables")
	fs.Bool("dry-run", false, "validate and print repair information without scheduling a repair")
	fs.Bool("show-tables", false, "print all table names for a keyspace")
	fs.Var(&IntensityFlag{Value: "0"}, "intensity",
		"repair speed, higher values result in higher speed and may increase cluster load, values between (0, 1) specifies percentage of active workers")
	taskInitCommonFlags(fs)
}

var repairIntensityCmd = &cobra.Command{
	Use:   "intensity <intensity>",
	Short: "Changes intensity of a running repair",
	Long: `This command changes speed of a running repair for provided cluster.

Higher intensity value results in higher repair speed and may increase cluster load. Values between (0, 1) specifies percentage of active workers.
Intensity must be a real number between (0-1) or integer when >= 1.
`,
	Args: cobra.ExactArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		var (
			intensity float64
			err       error
		)

		if err := validateIntensity(args[0]); err != nil {
			return err
		}

		intensity, err = strconv.ParseFloat(args[0], 64)
		if err != nil {
			return err
		}

		if err := client.SetRepairIntensity(ctx, cfgCluster, intensity); err != nil {
			if strings.Contains(err.Error(), "not found") {
				return errors.Errorf("not found any running repairs for '%s' cluster", cfgCluster)
			}
			return err
		}
		return nil
	},
}

func init() {
	cmd := repairIntensityCmd
	withScyllaDocs(cmd, "/sctool/#repair-intensity")
	register(cmd, repairCmd)
}

// IntensityFlag represents intensity flag which is a float64 value with a custom validation.
type IntensityFlag struct {
	Value string
}

// String returns intensity value as string.
func (fl *IntensityFlag) String() string {
	return fl.Value
}

// Set validates and sets intensity value.
func (fl *IntensityFlag) Set(s string) error {
	if err := validateIntensity(s); err != nil {
		return err
	}

	fl.Value = s
	return nil
}

// Type returns type of intensity.
func (fl *IntensityFlag) Type() string {
	return "float64"
}

func validateIntensity(s string) error {
	var errValidation = errors.New("intensity must be a real number between (0-1) or integer when >= 1")

	f, err := strconv.ParseFloat(s, 64)
	if err != nil {
		return errValidation
	}
	if f > 1 {
		_, err := strconv.ParseInt(s, 10, 64)
		if err != nil {
			return errValidation
		}
	}
	return nil
}
