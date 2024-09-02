// Copyright (C) 2017 ScyllaDB

package taskstop

import (
	_ "embed"

	"github.com/scylladb/scylla-manager/v3/pkg/command/flag"
	managerclient2 "github.com/scylladb/scylla-manager/v3/pkg/managerclient"
	"github.com/spf13/cobra"
	"gopkg.in/yaml.v2"
)

//go:embed res.yaml
var res []byte

type command struct {
	cobra.Command
	client *managerclient2.Client

	cluster string
	disable bool
}

func NewCommand(client *managerclient2.Client) *cobra.Command {
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
	w.Unwrap().BoolVar(&cmd.disable, "disable", false, "")
}

func (cmd *command) run(args []string) error {
	taskType, taskID, _, err := managerclient2.TaskSplit(args[0])
	if err != nil {
		return err
	}
	return cmd.client.StopTask(cmd.Context(), cmd.cluster, taskType, taskID, cmd.disable)
}
