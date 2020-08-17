// Copyright (C) 2017 ScyllaDB

package main

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/pkg/errors"
	"github.com/scylladb/mermaid/pkg/mermaidclient"
	"github.com/scylladb/mermaid/pkg/service/scheduler"
	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
)

var repairCmd = &cobra.Command{
	Use:   "repair",
	Short: "Schedules repair",
	RunE: func(cmd *cobra.Command, args []string) error {
		t := &mermaidclient.Task{
			Type:       "repair",
			Enabled:    true,
			Schedule:   new(mermaidclient.Schedule),
			Properties: make(map[string]interface{}),
		}

		return repairTaskUpdate(t, cmd)
	},
}

func repairTaskUpdate(t *mermaidclient.Task, cmd *cobra.Command) error {
	if err := commonFlagsUpdate(t, cmd); err != nil {
		return err
	}

	props := t.Properties.(map[string]interface{})

	failFast, err := cmd.Flags().GetBool("fail-fast")
	if err != nil {
		return err
	}
	if failFast {
		t.Schedule.NumRetries = 0
		props["fail_fast"] = true
	}

	t.Properties = props

	dryRun, err := cmd.Flags().GetBool("dry-run")
	if err != nil {
		return err
	}

	if f := cmd.Flag("intensity"); f.Changed {
		intensity, err := cmd.Flags().GetFloat64("intensity")
		if err != nil {
			return err
		}
		props["intensity"] = intensity
	}

	if f := cmd.Flag("small-table-threshold"); f.Changed {
		smallTableThreshold, err := cmd.Flags().GetString("small-table-threshold")
		if err != nil {
			return err
		}

		threshold, err := mermaidclient.ParseByteCount(smallTableThreshold)
		if err != nil {
			return err
		}

		props["small_table_threshold"] = threshold
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

	if t.ID == "" {
		id, err := client.CreateTask(ctx, cfgCluster, t)
		if err != nil {
			return err
		}
		t.ID = id.String()
	} else if err := client.UpdateTask(ctx, cfgCluster, t); err != nil {
		return err
	}

	fmt.Fprintln(cmd.OutOrStdout(), mermaidclient.TaskJoin(t.Type, t.ID))

	return nil
}

func init() {
	cmd := repairCmd
	withScyllaDocs(cmd, "/sctool/#repair")
	register(cmd, rootCmd)

	taskInitCommonFlags(repairFlags(cmd))
}

func repairFlags(cmd *cobra.Command) *pflag.FlagSet {
	fs := cmd.Flags()
	fs.StringSliceP("keyspace", "K", nil,
		"a comma-separated `list` of keyspace/tables glob patterns, e.g. 'keyspace,!keyspace.table_prefix_*' used to include or exclude keyspaces from backup")
	fs.StringSlice("dc", nil, "a comma-separated `list` of datacenter glob patterns, e.g. 'dc1,!otherdc*', used to specify the DCs to include or exclude from repair")
	fs.Bool("fail-fast", false, "stop repair on first error")
	fs.Bool("dry-run", false, "validate and print repair information without scheduling a repair")
	fs.Bool("show-tables", false, "print all table names for a keyspace")
	fs.Var(&IntensityFlag{Value: 0}, "intensity",
		"integer >= 1 or a float between (0-1), higher values may result in higher speed or cluster load, values between (0, 1) specify percentage of nodes that are repaired at once.")
	fs.String("small-table-threshold", "1GiB", "enable small table optimization for tables of size lower than given threshold. Supported units [B, MiB, GiB, TiB]")
	return fs
}

var repairIntensityCmd = &cobra.Command{
	Use:   "intensity <intensity>",
	Short: "Changes intensity of a running repair",
	Long: `This command changes speed of a running repair for provided cluster.

Higher intensity value results in higher repair speed and may increase cluster load.
Values between (0, 1) specifies percentage of active workers.
Intensity must be a real number between (0-1) or integer when >= 1.
`,
	Args: cobra.ExactArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		var intensity IntensityFlag

		if err := intensity.Set(args[0]); err != nil {
			return err
		}

		if err := client.SetRepairIntensity(ctx, cfgCluster, intensity.Value); err != nil {
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
	Value float64
}

// String returns intensity value as string.
func (fl *IntensityFlag) String() string {
	return fmt.Sprint(fl.Value)
}

// Set validates and sets intensity value.
func (fl *IntensityFlag) Set(s string) error {
	if s == "max" {
		fl.Value = -1
		return nil
	}

	var errValidation = errors.New("intensity must be an integer >= 1 or a float between (0-1)")

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

	fl.Value = f
	return nil
}

// Type returns type of intensity.
func (fl *IntensityFlag) Type() string {
	return "float64"
}

var repairUpdateCmd = &cobra.Command{
	Use:   "update <type/task-id>",
	Short: "Modifies a repair task",
	Args:  cobra.ExactArgs(1),

	RunE: func(cmd *cobra.Command, args []string) error {
		taskType, taskID, err := mermaidclient.TaskSplit(args[0])
		if err != nil {
			return err
		}

		if scheduler.TaskType(taskType) != scheduler.RepairTask {
			return fmt.Errorf("repair update can't handle %s task", taskType)
		}

		t, err := client.GetTask(ctx, cfgCluster, taskType, taskID)
		if err != nil {
			return err
		}

		return repairTaskUpdate(t, cmd)
	},
}

func init() {
	cmd := repairUpdateCmd
	withScyllaDocs(cmd, "/sctool/#repair-update")
	register(cmd, repairCmd)
	fs := repairFlags(cmd)
	fs.StringP("enabled", "e", "true", "enabled")
	taskInitCommonFlags(fs)
}
