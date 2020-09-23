// Copyright (C) 2017 ScyllaDB

package main

import (
	"fmt"
	"strconv"

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

	if f := cmd.Flag("parallel"); f.Changed {
		parallel, err := cmd.Flags().GetInt64("parallel")
		if err != nil {
			return err
		}
		props["parallel"] = parallel
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
		"a comma-separated `list` of keyspace/tables glob patterns, e.g. 'keyspace,!keyspace.table_prefix_*' used to include or exclude keyspaces from repair")
	fs.StringSlice("dc", nil, "a comma-separated `list` of datacenter glob patterns, e.g. 'dc1,!otherdc*', used to specify the DCs to include or exclude from repair")
	fs.Bool("fail-fast", false, "stop repair on first error")
	fs.Bool("dry-run", false, "validate and print repair information without scheduling a repair")
	fs.Bool("show-tables", false, "print all table names for a keyspace. Used only in conjunction with --dry-run")
	fs.Float64("intensity", 1,
		`integer >= 1 or a decimal between (0,1), higher values may result in higher speed and cluster load. 0 value means repair at maximum intensity`)
	fs.Int64("parallel", 0,
		`The maximum number of repair jobs to run in parallel, each node can participate in at most one repair at any given time.
Default is means system will repair at maximum parallelism`)
	fs.String("small-table-threshold", "1GiB", "enable small table optimization for tables of size lower than given threshold. Supported units [B, MiB, GiB, TiB]")
	return fs
}

var repairControlCmd = &cobra.Command{
	Use:   "control",
	Short: "Changes settings of running repairs to control speed and load",
	Long: `This command controls speed of a running repair for a provided cluster.

Higher intensity value results in higher repair speed and may increase cluster load.
Intensity must be a decimal number between (0,1) or integer when >= 1.

Higher parallel may result in higher repair speed and may increase cluster load.
By default repair will use the maximum possible parallelism.
`,
	RunE: func(cmd *cobra.Command, args []string) error {

		if !cmd.Flag("intensity").Changed && !cmd.Flag("parallel").Changed {
			return errors.New("at least one of intensity or parallel flags needs to be specified")
		}

		if f := cmd.Flag("intensity"); f.Changed {
			i, err := cmd.Flags().GetFloat64("intensity")
			if err != nil {
				return err
			}
			if err := client.SetRepairIntensity(ctx, cfgCluster, i); err != nil {
				return err
			}
		}

		if f := cmd.Flag("parallel"); f.Changed {
			p, err := cmd.Flags().GetInt64("parallel")
			if err != nil {
				return err
			}
			if err := client.SetRepairParallel(ctx, cfgCluster, p); err != nil {
				return err
			}
		}

		return nil
	},
}

func init() {
	cmd := repairControlCmd
	withScyllaDocs(cmd, "/sctool/#repair-control")
	fs := cmd.Flags()
	fs.Var(&IntensityFlag{Value: 1}, "intensity",
		`integer >= 1 or a decimal between (0,1), higher values may result in higher speed and cluster load. 0 value means repair at maximum intensity.
If not set no changes will be applied`)
	fs.Int64("parallel", 0,
		`The maximum number of repair jobs to run in parallel, each node can participate in at most one repair at any given time.
Default is means system will repair at maximum parallelism. If not set no changes will be applied`)
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
	var errValidation = errors.New("intensity must be an integer >= 1 or a decimal between (0,1)")

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
