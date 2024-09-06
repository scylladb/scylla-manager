// Copyright (C) 2017 ScyllaDB

package repair

import (
	_ "embed"
	"errors"
	"fmt"

	"github.com/scylladb/scylla-manager/v3/pkg/command/flag"
	"github.com/scylladb/scylla-manager/v3/pkg/managerclient"
	"github.com/spf13/cobra"
	"gopkg.in/yaml.v2"
)

//go:embed res.yaml
var res []byte

//go:embed update-res.yaml
var updateRes []byte

type command struct {
	flag.TaskBase
	client *managerclient.Client

	cluster             string
	dc                  []string
	keyspace            []string
	failFast            bool
	host                string
	ignoreDownHosts     bool
	intensity           *flag.Intensity
	parallel            int
	smallTableThreshold managerclient.SizeSuffix
	dryRun              bool
	showTables          bool
}

func NewCommand(client *managerclient.Client) *cobra.Command {
	cmd := newCommand(client, false)
	updateCmd := newCommand(client, true)
	cmd.AddCommand(&updateCmd.Command)

	return &cmd.Command
}

func newCommand(client *managerclient.Client, update bool) *command {
	var (
		cmd = &command{
			client:              client,
			intensity:           flag.NewIntensity(1),
			smallTableThreshold: 1073741824, // 1G
		}
		r []byte
	)
	if update {
		cmd.TaskBase = flag.NewUpdateTaskBase()
		r = updateRes
	} else {
		cmd.TaskBase = flag.MakeTaskBase()
		r = res
	}
	if err := yaml.Unmarshal(r, &cmd.Command); err != nil {
		panic(err)
	}
	cmd.init()
	cmd.RunE = func(_ *cobra.Command, args []string) error {
		return cmd.run(args)
	}
	return cmd
}

func (cmd *command) init() {
	cmd.TaskBase.Init()

	defer flag.MustSetUsages(&cmd.Command, res, "cluster")
	w := flag.Wrap(cmd.Flags())
	w.Cluster(&cmd.cluster)
	w.Datacenter(&cmd.dc)
	w.KeyspaceWithDefaultV(&cmd.keyspace, []string{"*", "!system_traces"})
	w.FailFast(&cmd.failFast)
	w.Unwrap().StringVar(&cmd.host, "host", "", "")
	w.Unwrap().BoolVar(&cmd.ignoreDownHosts, "ignore-down-hosts", false, "")
	w.Unwrap().Var(cmd.intensity, "intensity", "")
	w.Unwrap().IntVar(&cmd.parallel, "parallel", 0, "")
	w.Unwrap().Var(&cmd.smallTableThreshold, "small-table-threshold", "")
	w.Unwrap().BoolVar(&cmd.dryRun, "dry-run", false, "")
	w.Unwrap().BoolVar(&cmd.showTables, "show-tables", false, "")
}

func (cmd *command) run(args []string) error {
	var (
		task *managerclient.Task
		ok   bool
	)

	if cmd.Update() {
		a := managerclient.RepairTask
		if len(args) > 0 {
			a = args[0]
		}
		taskType, taskID, err := cmd.client.TaskSplit(cmd.Context(), cmd.cluster, a)
		if err != nil {
			return err
		}
		if taskType != managerclient.RepairTask {
			return fmt.Errorf("can't handle %s task", taskType)
		}

		task, err = cmd.client.GetTask(cmd.Context(), cmd.cluster, taskType, taskID)
		if err != nil {
			return err
		}
		ok = cmd.UpdateTask(task)
	} else {
		task = cmd.CreateTask(managerclient.RepairTask)
	}

	props := task.Properties.(map[string]interface{})

	if cmd.Flag("dc").Changed {
		props["dc"] = cmd.dc
		ok = true
	}
	if cmd.Flag("keyspace").Changed {
		props["keyspace"] = cmd.keyspace
		ok = true
	}
	if cmd.Flag("fail-fast").Changed {
		task.Schedule.NumRetries = 0
		props["fail_fast"] = cmd.failFast
		ok = true
	}
	if cmd.Flag("host").Changed {
		props["host"] = cmd.host
		ok = true
	}
	if cmd.Flag("ignore-down-hosts").Changed {
		props["ignore_down_hosts"] = cmd.ignoreDownHosts
		ok = true
	}
	if cmd.Flag("intensity").Changed {
		props["intensity"] = cmd.intensity.Value()
		ok = true
	}
	if cmd.Flag("parallel").Changed {
		props["parallel"] = cmd.parallel
		ok = true
	}
	if cmd.Flag("small-table-threshold").Changed {
		props["small_table_threshold"] = int64(cmd.smallTableThreshold)
		ok = true
	}

	if cmd.dryRun {
		res, err := cmd.client.GetRepairTarget(cmd.Context(), cmd.cluster, task)
		if err != nil {
			return err
		}
		res.Schedule = task.Schedule
		if cmd.showTables {
			res.ShowTables = -1
		}

		fmt.Fprintf(cmd.OutOrStderr(), "NOTICE: dry run mode, repair is not scheduled\n\n")
		return res.Render(cmd.OutOrStdout())
	}

	switch {
	case task.ID == "":
		id, err := cmd.client.CreateTask(cmd.Context(), cmd.cluster, task)
		if err != nil {
			return err
		}
		task.ID = id.String()
	case ok:
		if err := cmd.client.UpdateTask(cmd.Context(), cmd.cluster, task); err != nil {
			return err
		}
	default:
		return errors.New("nothing to do")
	}

	fmt.Fprintln(cmd.OutOrStdout(), managerclient.TaskID(task))
	return nil
}
