// Copyright (C) 2017 ScyllaDB

package tasks

import (
	_ "embed"
	"sort"
	"time"

	"github.com/go-openapi/strfmt"
	"github.com/pkg/errors"
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

	cluster   string
	all       bool
	showIDs   bool
	showProps bool
	status    string
	taskType  string
	sortKey   string
}

func NewCommand(client *managerclient2.Client) *cobra.Command {
	cmd := &command{
		client: client,
	}
	if err := yaml.Unmarshal(res, &cmd.Command); err != nil {
		panic(err)
	}
	cmd.init()
	cmd.RunE = func(_ *cobra.Command, args []string) error {
		return cmd.run()
	}
	return &cmd.Command
}

func (cmd *command) init() {
	defer flag.MustSetUsages(&cmd.Command, res)

	w := flag.Wrap(cmd.Flags())
	w.Cluster(&cmd.cluster)
	w.Unwrap().BoolVarP(&cmd.all, "all", "a", false, "")
	w.Unwrap().BoolVar(&cmd.showIDs, "show-ids", false, "")
	w.Unwrap().BoolVar(&cmd.showProps, "show-properties", false, "")
	w.Unwrap().StringVarP(&cmd.status, "status", "s", "", "")
	w.Unwrap().StringVarP(&cmd.taskType, "type", "t", "", "")
	w.Unwrap().StringVar(&cmd.sortKey, "sort", "", "")
}

func (cmd *command) run() error {
	if err := validateSortKey(cmd.sortKey); err != nil {
		return err
	}

	var clusters []*managerclient2.Cluster
	if cmd.cluster == "" {
		var err error
		clusters, err = cmd.client.ListClusters(cmd.Context())
		if err != nil {
			return err
		}
	} else {
		clusters = []*managerclient2.Cluster{{ID: cmd.cluster}}
	}

	w := cmd.OutOrStdout()
	h := func(clusterID string) error {
		tasks, err := cmd.client.ListTasks(cmd.Context(), clusterID, cmd.taskType, cmd.all, cmd.status, "")
		if err != nil {
			return err
		}
		tasks.ShowIDs = cmd.showIDs
		tasks.ShowProps = cmd.showProps

		sortTasks(tasks, taskListSortKey(cmd.sortKey))
		return tasks.Render(w)
	}
	for _, c := range clusters {
		if cmd.cluster == "" {
			managerclient2.FormatClusterName(w, c)
		}
		if err := h(c.ID); err != nil {
			managerclient2.PrintError(w, err)
		}
	}

	return nil
}

type taskListSortKey string

const (
	taskListSortNextActivation taskListSortKey = "next-activation"
	taskListSortStatus         taskListSortKey = "status"
)

var allTaskSortKeys = []taskListSortKey{taskListSortNextActivation, taskListSortStatus}

var tasksSortFunctions = map[taskListSortKey]func(tasks managerclient2.TaskListItemSlice){
	taskListSortNextActivation: sortTasksByNextActivation,
	taskListSortStatus:         sortTasksByStatus,
}

func validateSortKey(sortKey string) error {
	if sortKey == "" {
		return nil
	}

	for _, sk := range allTaskSortKeys {
		if string(sk) == sortKey {
			return nil
		}
	}
	return errors.Errorf("%s sort key not supported", sortKey)
}

func sortTasks(tasks managerclient2.TaskListItems, key taskListSortKey) {
	if key == "" {
		return
	}
	tasksSortFunctions[key](tasks.TaskListItemSlice)
}

func timeLessFunc(a, b *strfmt.DateTime) bool {
	var at, bt time.Time
	if a != nil {
		at = time.Time(*a)
	}
	if b != nil {
		bt = time.Time(*b)
	}
	return at.Before(bt)
}

func sortTasksByNextActivation(tasks managerclient2.TaskListItemSlice) {
	sort.Slice(tasks, func(i, j int) bool {
		return timeLessFunc(tasks[i].NextActivation, tasks[j].NextActivation)
	})
}

var taskStatusSortOrder = map[string]int{
	"NEW":      1,
	"RUNNING":  2,
	"STOPPING": 3,
	"STOPPED":  4,
	"WAITING":  5,
	"DONE":     6,
	"ERROR":    7,
	"ABORTED":  8,
}

func sortTasksByStatus(tasks managerclient2.TaskListItemSlice) {
	sort.Slice(tasks, func(i, j int) bool {
		return taskStatusSortOrder[tasks[i].Status] < taskStatusSortOrder[tasks[j].Status]
	})
}
