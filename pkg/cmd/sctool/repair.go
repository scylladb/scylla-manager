// Copyright (C) 2017 ScyllaDB

package main

import (
	"fmt"
	"strconv"

	"github.com/pkg/errors"
	"github.com/scylladb/scylla-manager/pkg/managerclient"
	"github.com/scylladb/scylla-manager/pkg/service/scheduler"
	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
)

const parallelLongDesc = `
The --parallel flag specifies the maximum number of Scylla repair jobs that can run at the same time (on different token ranges and replicas).
Each node can take part in at most one repair at any given moment. By default the maximum possible parallelism is used.
The effective parallelism depends on a keyspace replication factor (RF) and the number of nodes.
The formula to calculate it is as follows: number of nodes / RF, ex. for 6 node cluster with RF=3 the maximum parallelism is 2.`

const intensityLongDesc = `
The --intensity flag specifies how many token ranges (per shard) to repair in a single Scylla repair job. By default this is 1.
If you set it to 0 the number of token ranges is adjusted to the maximum supported by node (see max_repair_ranges_in_parallel in Scylla logs).
Valid values are 0 and integers >= 1. Higher values will result in increased cluster load and slightly faster repairs.
Changing the intensity impacts repair granularity if you need to resume it, the higher the value the more work on resume.

For Scylla clusters that DO NOT SUPPORT ROW-LEVEL REPAIR, intensity can be a decimal between (0,1).
In that case it specifies percent of shards that can be repaired in parallel on a repair master node.
For Scylla clusters that are row-level repair enabled, setting intensity below 1 has the same effect as setting intensity 1.`

var repairCmd = &cobra.Command{
	Use:   "repair",
	Short: "Schedules repairs",
	Long: `Repair speed is controlled by two flags --parallel and --intensity.
The values of those flags can be adjusted while a repair is running using the 'sctool repair control' command.
` + parallelLongDesc + `
` + intensityLongDesc,

	RunE: func(cmd *cobra.Command, args []string) error {
		t := &managerclient.Task{
			Type:       "repair",
			Enabled:    true,
			Schedule:   new(managerclient.Schedule),
			Properties: make(map[string]interface{}),
		}

		return repairTaskUpdate(t, cmd)
	},
}

func repairTaskUpdate(t *managerclient.Task, cmd *cobra.Command) error {
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

	if f := cmd.Flag("host"); f.Changed {
		host, err := cmd.Flags().GetString("host")
		if err != nil {
			return err
		}
		props["host"] = host
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

		threshold, err := managerclient.ParseByteCount(smallTableThreshold)
		if err != nil {
			return err
		}

		props["small_table_threshold"] = threshold
	}

	dryRun, err := cmd.Flags().GetBool("dry-run")
	if err != nil {
		return err
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

	fmt.Fprintln(cmd.OutOrStdout(), managerclient.TaskJoin(t.Type, t.ID))

	return nil
}

func init() {
	cmd := repairCmd
	taskInitCommonFlags(repairFlags(cmd))
	register(cmd, rootCmd)
}

func repairFlags(cmd *cobra.Command) *pflag.FlagSet {
	fs := cmd.Flags()
	fs.StringSliceP("keyspace", "K", nil,
		"a comma-separated `list` of keyspace/tables glob patterns, e.g. 'keyspace,!keyspace.table_prefix_*' used to include or exclude keyspaces from repair")
	fs.StringSlice("dc", nil, "a comma-separated `list` of datacenter glob patterns, e.g. 'dc1,!otherdc*', used to specify the DCs to include or exclude from repair")
	fs.Bool("dry-run", false, "validate and print repair information without scheduling a repair")
	fs.Bool("fail-fast", false, "stop repair on first error")
	fs.String("host", "", "host to repair, by default all hosts are repaired")
	fs.Bool("show-tables", false, "print all table names for a keyspace. Used only in conjunction with --dry-run")
	fs.Var(&IntensityFlag{Value: 1}, "intensity", "how many token ranges (per shard) to repair in a single Scylla repair job, see the command description for details")
	fs.String("small-table-threshold", "1GiB", "enable small table optimization for tables of size lower than given threshold. Supported units [B, MiB, GiB, TiB]")
	fs.Int64("parallel", 0, "limit of parallel repair jobs, full parallelism by default, see the command description for details")
	return fs
}

var repairControlCmd = &cobra.Command{
	Use:   "control",
	Short: "Changes repair parameters on the flight",
	Long: `Changes repair parameters on the flight
` + parallelLongDesc + `
` + intensityLongDesc,
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
	fs := cmd.Flags()
	fs.Var(&IntensityFlag{Value: 1}, "intensity", "how many token ranges (per shard) to repair in a single Scylla repair job, see the command description for details")
	fs.Int64("parallel", 0, "limit of parallel repair jobs, full parallelism by default, see the command description for details")
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
	Long: `Modifies a repair task

Repair speed is controlled by two flags --parallel and --intensity.
The values of those flags can be adjusted while a repair is running using the 'sctool repair control' command.
` + parallelLongDesc + `
` + intensityLongDesc,
	Args: cobra.ExactArgs(1),

	RunE: func(cmd *cobra.Command, args []string) error {
		taskType, taskID, err := managerclient.TaskSplit(args[0])
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
	fs := repairFlags(cmd)
	fs.StringP("enabled", "e", "true", "enabled")
	taskInitCommonFlags(fs)
	register(cmd, repairCmd)
}
