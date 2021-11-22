// Copyright (C) 2017 ScyllaDB

package backup

import (
	_ "embed"
	"fmt"
	"time"

	"github.com/scylladb/scylla-manager/pkg/command/flag"
	"github.com/scylladb/scylla-manager/pkg/managerclient"
	"github.com/scylladb/scylla-manager/pkg/service/scheduler"
	"github.com/spf13/cobra"
	"go.uber.org/atomic"
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
	keyspace         []string
	retention        int
	rateLimit        []string
	snapshotParallel []string
	uploadParallel   []string
	dryRun           bool
	showTables       bool
	purgeOnly        bool
}

func NewCommand(client *managerclient.Client) *cobra.Command {
	cmd := newCommand(client, false)
	updateCmd := newCommand(client, true)
	cmd.AddCommand(&updateCmd.Command)

	return &cmd.Command
}

func newCommand(client *managerclient.Client, update bool) *command {
	var r []byte
	if update {
		r = updateRes
	} else {
		r = res
	}

	cmd := &command{
		client: client,
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
	defer flag.MustSetUsages(&cmd.Command, res, "cluster", "location")

	w := flag.Wrap(cmd.Flags())
	w.Cluster(&cmd.cluster)
	w.Location(&cmd.location)
	w.Datacenter(&cmd.dc)
	w.Keyspace(&cmd.keyspace)
	w.Unwrap().IntVar(&cmd.retention, "retention", 7, "")
	w.Unwrap().StringSliceVar(&cmd.rateLimit, "rate-limit", nil, "")
	w.Unwrap().StringSliceVar(&cmd.snapshotParallel, "snapshot-parallel", nil, "")
	w.Unwrap().StringSliceVar(&cmd.uploadParallel, "upload-parallel", nil, "")
	w.Unwrap().BoolVar(&cmd.dryRun, "dry-run", false, "")
	w.Unwrap().BoolVar(&cmd.showTables, "show-tables", false, "")
	w.Unwrap().BoolVar(&cmd.purgeOnly, "purge-only", false, "")
}

func (cmd *command) run(args []string) error {
	var task *managerclient.Task

	if cmd.Update() {
		taskType, taskID, err := managerclient.TaskSplit(args[0])
		if err != nil {
			return err
		}
		if scheduler.TaskType(taskType) != "backup" {
			return fmt.Errorf("can't handle %s task", taskType)
		}
		task, err = cmd.client.GetTask(cmd.Context(), cmd.cluster, taskType, taskID)
		if err != nil {
			return err
		}
		cmd.UpdateTask(task)
	} else {
		task = &managerclient.Task{
			Type:       "backup",
			Enabled:    cmd.Enabled(),
			Schedule:   cmd.Schedule(),
			Properties: make(map[string]interface{}),
		}
	}

	props := task.Properties.(map[string]interface{})
	if len(cmd.location) > 0 {
		props["location"] = cmd.location
	}
	if len(cmd.dc) > 0 {
		props["dc"] = cmd.dc
	}
	if len(cmd.keyspace) > 0 {
		props["keyspace"] = cmd.keyspace
	}
	if cmd.Flag("retention").Changed {
		props["retention"] = cmd.retention
	}
	if len(cmd.rateLimit) > 0 {
		props["rate_limit"] = cmd.rateLimit
	}
	if len(cmd.snapshotParallel) > 0 {
		props["snapshot_parallel"] = cmd.snapshotParallel
	}
	if len(cmd.uploadParallel) > 0 {
		props["upload_parallel"] = cmd.uploadParallel
	}
	if cmd.Flag("purge-only").Changed {
		props["purge_only"] = cmd.purgeOnly
	}

	if cmd.dryRun {
		stillWaiting := atomic.NewBool(true)
		time.AfterFunc(5*time.Second, func() {
			if stillWaiting.Load() {
				fmt.Fprintf(cmd.OutOrStderr(), "NOTICE: this may take a while, we are performing disk size calculations on the nodes\n")
			}
		})

		res, err := cmd.client.GetBackupTarget(cmd.Context(), cmd.cluster, task)
		stillWaiting.Store(false)
		if err != nil {
			return err
		}

		fmt.Fprintf(cmd.OutOrStderr(), "NOTICE: dry run mode, backup is not scheduled\n\n")
		if cmd.showTables {
			res.ShowTables = -1
		}
		return res.Render(cmd.OutOrStdout())
	}

	if task.ID == "" {
		id, err := cmd.client.CreateTask(cmd.Context(), cmd.cluster, task)
		if err != nil {
			return err
		}
		task.ID = id.String()
	} else if err := cmd.client.UpdateTask(cmd.Context(), cmd.cluster, task); err != nil {
		return err
	}

	fmt.Fprintln(cmd.OutOrStdout(), managerclient.TaskJoin(task.Type, task.ID))
	return nil
}
