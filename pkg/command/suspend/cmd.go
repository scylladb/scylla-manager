// Copyright (C) 2017 ScyllaDB

package suspend

import (
	_ "embed"
	"fmt"

	"github.com/pkg/errors"
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

	cluster    string
	duration   flag.Duration
	startTasks bool
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
	w.Unwrap().Var(&cmd.duration, "duration", "")
	w.Unwrap().BoolVar(&cmd.startTasks, "on-resume-start-tasks", false, "")
}

func (cmd *command) run(args []string) error {
	// On plain suspend call do not create a task.
	if !cmd.Update() && cmd.duration.Value() == 0 {
		if cmd.startTasks {
			return errors.New("can't use startTasks without a duration")
		}
		t := cmd.CreateTask(managerclient.SuspendTask)
		if t.Schedule.Cron == "" && t.Schedule.StartDate == nil {
			return cmd.client.Suspend(cmd.Context(), cmd.cluster)
		}
	}

	var (
		task *managerclient.Task
		ok   bool
	)

	if cmd.Update() {
		a := managerclient.SuspendTask
		if len(args) > 0 {
			a = args[0]
		}
		taskType, taskID, err := cmd.client.TaskSplit(cmd.Context(), cmd.cluster, a)
		if err != nil {
			return err
		}
		if taskType != managerclient.SuspendTask {
			return fmt.Errorf("can't handle %s task", taskType)
		}

		task, err = cmd.client.GetTask(cmd.Context(), cmd.cluster, taskType, taskID)
		if err != nil {
			return err
		}
		ok = cmd.UpdateTask(task)
	} else {
		task = cmd.CreateTask(managerclient.SuspendTask)
	}

	props := task.Properties.(map[string]interface{})

	if cmd.Flag("duration").Changed {
		props["duration"] = cmd.duration.Value()
		ok = true
	}
	if cmd.Flag("on-resume-start-tasks").Changed {
		props["start_tasks"] = cmd.startTasks
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

	fmt.Fprintln(cmd.OutOrStdout(), managerclient.TaskID(task))
	return nil
}
