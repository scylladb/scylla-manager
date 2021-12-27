// Copyright (C) 2017 ScyllaDB

package backupvalidate

import (
	_ "embed"
	"fmt"

	"github.com/pkg/errors"
	"github.com/scylladb/scylla-manager/pkg/command/flag"
	"github.com/scylladb/scylla-manager/pkg/managerclient"
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
	location            []string
	deleteOrphanedFiles bool
	parallel            int
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
	w.Location(&cmd.location)
	w.Unwrap().BoolVar(&cmd.deleteOrphanedFiles, "delete-orphaned-files", false, "")
	w.Unwrap().IntVar(&cmd.parallel, "parallel", 0, "")
}

func (cmd *command) run(args []string) error {
	var (
		task *managerclient.Task
		ok   bool
	)

	if cmd.Update() {
		a := managerclient.ValidateBackupTask
		if len(args) > 0 {
			a = args[0]
		}
		taskType, taskID, err := cmd.client.TaskSplit(cmd.Context(), cmd.cluster, a)
		if err != nil {
			return err
		}
		if taskType != managerclient.ValidateBackupTask {
			return fmt.Errorf("can't handle %s task", taskType)
		}
		task, err = cmd.client.GetTask(cmd.Context(), cmd.cluster, taskType, taskID)
		if err != nil {
			return err
		}
		ok = cmd.UpdateTask(task)
	} else {
		task = cmd.CreateTask(managerclient.ValidateBackupTask)
	}

	props := task.Properties.(map[string]interface{})
	if cmd.Flag("location").Changed {
		props["location"] = cmd.location
		ok = true
	}
	if cmd.Flag("delete-orphaned-files").Changed {
		props["delete_orphaned_files"] = cmd.deleteOrphanedFiles
		ok = true
	}
	if cmd.Flag("parallel").Changed {
		props["parallel"] = cmd.parallel
		ok = true
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

	fmt.Fprintln(cmd.OutOrStdout(), managerclient.TaskJoin(task.Type, task.ID))
	return nil
}
