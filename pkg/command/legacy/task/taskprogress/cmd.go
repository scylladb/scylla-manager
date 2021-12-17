// Copyright (C) 2017 ScyllaDB

package taskprogress

import (
	_ "embed"

	"github.com/scylladb/scylla-manager/pkg/command/flag"
	"github.com/scylladb/scylla-manager/pkg/managerclient"
	"github.com/scylladb/scylla-manager/pkg/util/uuid"
	"github.com/spf13/cobra"
	"gopkg.in/yaml.v2"
)

//go:embed res.yaml
var res []byte

type command struct {
	cobra.Command
	client *managerclient.Client

	cluster  string
	keyspace []string
	details  bool
	host     []string
	runID    string
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
	w.Keyspace(&cmd.keyspace)
	w.Unwrap().BoolVar(&cmd.details, "details", false, "")
	w.Unwrap().StringSliceVar(&cmd.host, "host", nil, "")
	w.Unwrap().StringVar(&cmd.runID, "run", "latest", "Show progress of a particular run, see sctool task history to get the IDs.")
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

	if cmd.runID != "latest" {
		if _, err = uuid.Parse(cmd.runID); err != nil {
			return err
		}
	}

	switch taskType {
	case managerclient.RepairTask:
		return cmd.renderRepairProgress(task)
	case managerclient.BackupTask:
		return cmd.renderBackupProgress(task)
	case managerclient.ValidateBackupTask:
		return cmd.renderValidateBackupProgress(task)
	}

	return nil
}

func (cmd *command) renderRepairProgress(t *managerclient.Task) error {
	p, err := cmd.client.RepairProgress(cmd.Context(), cmd.cluster, t.ID, cmd.runID)
	if err != nil {
		return err
	}

	p.Detailed = cmd.details
	if err := p.SetHostFilter(cmd.host); err != nil {
		return err
	}
	if err := p.SetKeyspaceFilter(cmd.keyspace); err != nil {
		return err
	}
	p.Task = t

	return p.Render(cmd.OutOrStdout())
}

func (cmd *command) renderBackupProgress(t *managerclient.Task) error {
	p, err := cmd.client.BackupProgress(cmd.Context(), cmd.cluster, t.ID, cmd.runID)
	if err != nil {
		return err
	}

	p.Detailed = cmd.details
	if err := p.SetHostFilter(cmd.host); err != nil {
		return err
	}
	if err := p.SetKeyspaceFilter(cmd.keyspace); err != nil {
		return err
	}
	p.Task = t
	p.AggregateErrors()

	return p.Render(cmd.OutOrStdout())
}

func (cmd *command) renderValidateBackupProgress(t *managerclient.Task) error {
	p, err := cmd.client.ValidateBackupProgress(cmd.Context(), cmd.cluster, t.ID, cmd.runID)
	if err != nil {
		return err
	}

	p.Detailed = cmd.details
	if err := p.SetHostFilter(cmd.host); err != nil {
		return err
	}
	p.Task = t

	return p.Render(cmd.OutOrStdout())
}
