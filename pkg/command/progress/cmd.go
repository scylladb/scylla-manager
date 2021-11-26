// Copyright (C) 2017 ScyllaDB

package progress

import (
	_ "embed"
	"strings"

	"github.com/pkg/errors"
	"github.com/scylladb/go-set/strset"
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

const (
	latest = "latest"
	tilde  = "~"
)

func (cmd *command) init() {
	defer flag.MustSetUsages(&cmd.Command, res, "cluster")

	w := flag.Wrap(cmd.Flags())
	w.Cluster(&cmd.cluster)
	w.Keyspace(&cmd.keyspace)
	w.Unwrap().BoolVar(&cmd.details, "details", false, "")
	w.Unwrap().StringSliceVar(&cmd.host, "host", nil, "")
	w.Unwrap().StringVar(&cmd.runID, "run", latest, "Show progress of a particular run, see sctool info to get the `ID`s.")
}

const (
	backupTask         = "backup"
	repairTask         = "repair"
	validateBackupTask = "validate_backup"
)

var supportedTaskTypes = strset.New(backupTask, repairTask, validateBackupTask)

func (cmd *command) run(args []string) error {
	var (
		taskType string
		taskID   uuid.UUID
		err      error
	)

	if supportedTaskTypes.Has(args[0]) {
		taskType = args[0]
	} else {
		taskType, taskID, err = managerclient.TaskSplit(args[0])
		if err != nil {
			return err
		}
	}

	if taskID == uuid.Nil {
		tasks, err := cmd.client.ListTasks(cmd.Context(), cmd.cluster, taskType, false, "")
		if err != nil {
			return err
		}
		switch len(tasks.ExtendedTaskSlice) {
		case 0:
			return errors.Errorf("no task of type %s", taskType)
		case 1:
			taskID, err = uuid.Parse(tasks.ExtendedTaskSlice[0].ID)
			if err != nil {
				return err
			}
		default:
			ids := make([]string, len(tasks.ExtendedTaskSlice))
			for i, t := range tasks.ExtendedTaskSlice {
				ids[i] = "- " + managerclient.TaskJoin(taskType, t.ID)
			}
			return errors.Errorf("task ambiguity run with one of:\n%s", strings.Join(ids, "\n"))
		}
	}

	task, err := cmd.client.GetTask(cmd.Context(), cmd.cluster, taskType, taskID)
	if err != nil {
		return err
	}

	if cmd.runID != latest && !strings.HasPrefix(cmd.runID, tilde) {
		if _, err = uuid.Parse(cmd.runID); err != nil {
			return err
		}
	}

	switch taskType {
	case repairTask:
		return cmd.renderRepairProgress(task)
	case backupTask:
		return cmd.renderBackupProgress(task)
	case validateBackupTask:
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
