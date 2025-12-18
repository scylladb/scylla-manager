// Copyright (C) 2025 ScyllaDB

package tablet

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

	cluster string
}

func NewCommand(client *managerclient.Client) *cobra.Command {
	cmd, err := newCommand(client, false)
	if err != nil {
		// You could print/log this error, but for CLI construction,
		// it's often best to panic or return some placeholder Command.
		panic(fmt.Errorf("failed to construct tablet repair command: %w", err))
	}
	updateCmd, err := newCommand(client, true)
	if err != nil {
		panic(fmt.Errorf("failed to construct tablet repair update subcommand: %w", err))
	}
	cmd.AddCommand(&updateCmd.Command)

	return &cmd.Command
}

func newCommand(client *managerclient.Client, update bool) (*command, error) {
	var (
		cmd = &command{
			client: client,
		}
		resourceBytes []byte
	)
	if update {
		cmd.TaskBase = flag.NewUpdateTaskBase()
		resourceBytes = updateRes
	} else {
		cmd.TaskBase = flag.MakeTaskBase()
		resourceBytes = res
	}
	if err := yaml.Unmarshal(resourceBytes, &cmd.Command); err != nil {
		return nil, err
	}
	cmd.init()
	cmd.RunE = func(_ *cobra.Command, args []string) error {
		return cmd.run(args)
	}
	return cmd, nil
}

func (cmd *command) init() {
	cmd.Init()

	defer flag.MustSetUsages(&cmd.Command, res, "cluster")
	w := flag.Wrap(cmd.Flags())
	w.Cluster(&cmd.cluster)
}

func (cmd *command) run(args []string) error {
	var (
		task *managerclient.Task
		ok   bool
	)

	if cmd.Update() {
		taskArg := managerclient.TabletRepairTask
		if len(args) > 0 {
			taskArg = args[0]
		}
		taskType, taskID, err := cmd.client.TaskSplit(cmd.Context(), cmd.cluster, taskArg)
		if err != nil {
			return err
		}
		if taskType != managerclient.TabletRepairTask {
			return fmt.Errorf("can't handle %s task", taskType)
		}

		task, err = cmd.client.GetTask(cmd.Context(), cmd.cluster, taskType, taskID)
		if err != nil {
			return err
		}
		ok = cmd.UpdateTask(task)
	} else {
		task = cmd.CreateTask(managerclient.TabletRepairTask)
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
		return errors.New("no task updates to apply")
	}

	_, err := fmt.Fprintln(cmd.OutOrStdout(), managerclient.TaskID(task))
	return err
}
