// Copyright (C) 2017 ScyllaDB

package restore

import (
	_ "embed"
	"fmt"

	"github.com/pkg/errors"
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

	cluster          string
	dc               []string
	location         []string
	snapshotTag      string
	keyspace         []string
	table            []string
	batchSize        int
	minFreeDiskSpace int // TODO - reconsider name?
	dryRun           bool

	// TODO - do we keep any of the below?
	// failFast            bool
	// host                string
	// ignoreDownHosts     bool
	// intensity           *flag.Intensity
	// parallel            int
	// smallTableThreshold managerclient.SizeSuffix
	// showTables          bool
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
			client: client,
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
	w.Location(&cmd.location)
	w.Keyspace(&cmd.keyspace)
	w.Unwrap().IntVar(&cmd.batchSize, "batch-size", 2, "")
	w.Unwrap().IntVar(&cmd.minFreeDiskSpace, "min-free-disk-space", 10, "")
	w.Unwrap().BoolVar(&cmd.dryRun, "dry-run", false, "")
	w.Unwrap().StringVar(&cmd.snapshotTag, "snapshot-tag", "", "")
	w.Unwrap().StringSliceVar(&cmd.table, "table", nil, "")
}

func (cmd *command) run(args []string) error {
	var (
		task *managerclient.Task
		ok   bool
	)

	if cmd.Update() {
		a := managerclient.RestoreTask
		if len(args) > 0 {
			a = args[0]
		}
		taskType, taskID, err := cmd.client.TaskSplit(cmd.Context(), cmd.cluster, a)
		if err != nil {
			return err
		}
		if taskType != managerclient.RestoreTask {
			return fmt.Errorf("can't handle %s task", taskType)
		}

		task, err = cmd.client.GetTask(cmd.Context(), cmd.cluster, taskType, taskID)
		if err != nil {
			return err
		}
		ok = cmd.UpdateTask(task)
	} else {
		task = cmd.CreateTask(managerclient.RestoreTask)
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

	if cmd.dryRun {
		res, err := cmd.client.GetRestoreTarget(cmd.Context(), cmd.cluster, task)
		if err != nil {
			return err
		}
		res.Schedule = task.Schedule

		fmt.Fprintf(cmd.OutOrStderr(), "NOTICE: dry run mode, restore is not scheduled\n\n")
		return res.Render(cmd.OutOrStdout())
	}

	if cmd.Flag("location").Changed {
		props["location"] = cmd.location
	}

	if cmd.Flag("batch-size").Changed {
		props["batch_size"] = cmd.batchSize
	}

	if cmd.Flag("min-free-disk-space").Changed {
		props["min_free_disk_space"] = cmd.minFreeDiskSpace
	}

	if cmd.Flag("table").Changed {
		props["table"] = cmd.table
	}
	if cmd.Flag("snapshot-tag").Changed {
		props["snapshot_tag"] = cmd.snapshotTag
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
