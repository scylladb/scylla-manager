// Copyright (C) 2017 ScyllaDB

package start

import (
	_ "embed"

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

	cluster    string
	noContinue bool
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
	defer flag.MustSetUsages(&cmd.Command, res)

	w := flag.Wrap(cmd.Flags())
	w.Cluster(&cmd.cluster)
	w.Unwrap().BoolVar(&cmd.noContinue, "no-continue", false, "")
}

func (cmd *command) run(args []string) error {
	taskType, taskID, err := managerclient.TaskSplit(args[0])
	if err != nil {
		return err
	}
	return cmd.client.StartTask(cmd.Context(), cmd.cluster, taskType, taskID, !cmd.noContinue)
}
