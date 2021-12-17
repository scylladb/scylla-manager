// Copyright (C) 2017 ScyllaDB

package taskupdate

import (
	_ "embed"

	"github.com/pkg/errors"
	"github.com/scylladb/scylla-manager/pkg/command/flag"
	"github.com/scylladb/scylla-manager/pkg/managerclient"
	"github.com/spf13/cobra"
	"gopkg.in/yaml.v2"
)

//go:embed res.yaml
var res []byte

type command struct {
	flag.TaskBase
	client *managerclient.Client

	cluster string
}

func NewCommand(client *managerclient.Client) *cobra.Command {
	cmd := &command{
		TaskBase: flag.NewUpdateTaskBase(),
		client:   client,
	}
	if err := yaml.Unmarshal(res, &cmd.Command); err != nil {
		panic(err)
	}
	cmd.init()
	cmd.RunE = func(_ *cobra.Command, args []string) error {
		return cmd.run(args)
	}
	return &cmd.Command
}

func (cmd *command) init() {
	defer flag.MustSetUsages(&cmd.Command, res, "cluster")

	w := flag.Wrap(cmd.Flags())
	w.Cluster(&cmd.cluster)
}

func (cmd *command) run(args []string) error {
	taskType, taskID, _, err := managerclient.TaskSplit(args[0])
	if err != nil {
		return err
	}

	task, err := cmd.client.GetTask(cmd.Context(), cmd.cluster, taskType, taskID)
	if err != nil {
		return err
	}

	if !cmd.UpdateTask(task) {
		return errors.New("nothing to change")
	}

	return cmd.client.UpdateTask(cmd.Context(), cmd.cluster, task)
}
