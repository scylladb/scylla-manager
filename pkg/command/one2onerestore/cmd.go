// Copyright (C) 2025 ScyllaDB

package one2onerestore

import (
	_ "embed"
	"fmt"
	"strings"

	"github.com/pkg/errors"
	"github.com/scylladb/scylla-manager/v3/pkg/command/flag"
	"github.com/scylladb/scylla-manager/v3/pkg/managerclient"
	"github.com/scylladb/scylla-manager/v3/pkg/util/uuid"
	"github.com/scylladb/scylla-manager/v3/swagger/gen/scylla-manager/models"
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

	cluster       string
	sourceCluster uuid.Value
	location      []string
	keyspace      []string
	snapshotTag   string
	nodesMapping  nodesMapping
	unpinAgentCPU bool
	dryRun        bool
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
	w.Keyspace(&cmd.keyspace)

	// Flags specific to 1-1-restore
	w.Unwrap().Var(&cmd.sourceCluster, "source-cluster-id", "")
	w.Unwrap().StringVarP(&cmd.snapshotTag, "snapshot-tag", "T", "", "")
	w.Unwrap().Var(&cmd.nodesMapping, "nodes-mapping", "")

	// Common configuration for restore procedures
	w.Unwrap().BoolVar(&cmd.dryRun, "dry-run", false, "")
	w.Unwrap().BoolVar(&cmd.unpinAgentCPU, "unpin-agent-cpu", false, "")
}

func (cmd *command) run(args []string) error {
	requiredFlags := []string{
		"location",
		"snapshot-tag",
		"source-cluster-id",
		"nodes-mapping",
	}

	if !cmd.Update() {
		// Since we have different required flags for create vs update commands,
		// cmd.MarkFlagRequired can't be used for both because it checks flags before knowing
		// whether the command is a create or update operation.
		if err := checkRequiredFlags(cmd, requiredFlags); err != nil {
			return err
		}
	}

	task, taskUpdated, err := createOrUpdateTask(cmd, args)
	if err != nil {
		return err
	}

	propsUpdated, err := flagsToTaskProperties(cmd, task)
	if err != nil {
		return err
	}

	if cmd.dryRun {
		return dryRun(cmd, task)
	}

	taskID, err := saveTask(cmd, task, taskUpdated || propsUpdated)
	if err != nil {
		return err
	}

	fmt.Fprintln(cmd.OutOrStdout(), taskID)
	return nil
}

func checkRequiredFlags(cmd *command, requiredFlags []string) error {
	var missingFlags []string
	for _, f := range requiredFlags {
		if cmd.Flag(f).Changed {
			continue
		}
		missingFlags = append(missingFlags, f)
	}

	if len(missingFlags) != 0 {
		return errors.Errorf("required flag(s) %s not set", strings.Join(missingFlags, ", "))
	}

	return nil
}

func createOrUpdateTask(cmd *command, args []string) (task *models.Task, taskUpdated bool, err error) {
	if cmd.Update() {
		a := managerclient.One2OneRestoreTask
		if len(args) > 0 {
			a = args[0]
		}
		taskType, taskID, err := cmd.client.TaskSplit(cmd.Context(), cmd.cluster, a)
		if err != nil {
			return nil, false, err
		}
		if taskType != managerclient.One2OneRestoreTask {
			return nil, false, fmt.Errorf("can't handle %s task", taskType)
		}
		task, err := cmd.client.GetTask(cmd.Context(), cmd.cluster, taskType, taskID)
		if err != nil {
			return nil, false, err
		}
		return task, cmd.UpdateTask(task), nil
	}
	return cmd.CreateTask(managerclient.One2OneRestoreTask), false, nil
}

func dryRun(cmd *command, _ *models.Task) error {
	fmt.Fprintf(cmd.OutOrStderr(), "NOTICE: dry run mode, one2onerestore is not yet implemented\n\n")
	return nil
}

func flagsToTaskProperties(cmd *command, task *models.Task) (updated bool, err error) {
	// Disallow updating one2onerestore task's core flags, since one2onerestore procedure cannot adjust itself to this change.
	forbiddenUpdate := func(flagName string) error {
		return errors.Errorf("updating one2onerestore task's '--%s' flag is forbidden. For this purpose, please create a new task with given properties", flagName)
	}

	type taskProperty struct {
		flagName     string
		value        any
		canBeUpdated bool
	}

	flagNameToPropName := func(f string) string {
		return strings.ReplaceAll(f, "-", "_")
	}

	taskProperties := []taskProperty{
		{
			flagName: "location",
			value:    cmd.location,
		},
		{
			flagName: "keyspace",
			value:    cmd.keyspace,
		},
		{
			flagName: "source-cluster-id",
			value:    cmd.sourceCluster.String(),
		},
		{
			flagName: "snapshot-tag",
			value:    cmd.snapshotTag,
		},
		{
			flagName: "nodes-mapping",
			value:    cmd.nodesMapping,
		},
		{
			flagName:     "unpin-agent-cpu",
			value:        cmd.unpinAgentCPU,
			canBeUpdated: true,
		},
	}

	props := task.Properties.(map[string]interface{})

	for _, p := range taskProperties {
		if cmd.Flag(p.flagName).Changed {
			if cmd.Update() && !p.canBeUpdated {
				return updated, forbiddenUpdate(p.flagName)
			}
			if p.canBeUpdated {
				updated = true
			}
			propName := flagNameToPropName(p.flagName)
			props[propName] = p.value
		}
	}

	return updated, nil
}

func saveTask(cmd *command, task *models.Task, taskUpdated bool) (string, error) {
	switch {
	case task.ID == "":
		id, err := cmd.client.CreateTask(cmd.Context(), cmd.cluster, task)
		if err != nil {
			return "", err
		}
		task.ID = id.String()
	case taskUpdated:
		if err := cmd.client.UpdateTask(cmd.Context(), cmd.cluster, task); err != nil {
			return "", err
		}
	default:
		return "", errors.New("nothing to do")
	}
	return managerclient.TaskID(task), nil
}
