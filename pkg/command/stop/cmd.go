// Copyright (C) 2017 ScyllaDB

package stop

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
	cobra.Command
	client *managerclient.Client

	cluster string
	delete  bool
	disable bool
}

func NewCommand(client *managerclient.Client) *cobra.Command {
	cmd := &command{
		client: client,
		Command: cobra.Command{
			Args: cobra.ExactArgs(1),
		},
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
	w.Unwrap().BoolVar(&cmd.delete, "delete", false, "")
	w.Unwrap().BoolVar(&cmd.disable, "disable", false, "")
}

func (cmd *command) run(args []string) error {
	taskType, taskID, err := cmd.client.TaskSplit(cmd.Context(), cmd.cluster, args[0])
	if err != nil {
		return err
	}
	if err := cmd.client.StopTask(cmd.Context(), cmd.cluster, taskType, taskID, cmd.disable); err != nil {
		return errors.Wrap(err, "stop")
	}
	if cmd.delete {
		if err := cmd.client.DeleteTask(cmd.Context(), cmd.cluster, taskType, taskID); err != nil {
			return errors.Wrap(err, "delete")
		}
	}
	return nil
}
