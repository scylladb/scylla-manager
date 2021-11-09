// Copyright (C) 2017 ScyllaDB

package tasklist

import (
	_ "embed"
	"sort"
	"time"

	"github.com/go-openapi/strfmt"
	"github.com/pkg/errors"
	"github.com/scylladb/scylla-manager/pkg/command/flag"
	"github.com/scylladb/scylla-manager/pkg/managerclient"
	"github.com/scylladb/scylla-manager/pkg/util/timeutc"
	"github.com/spf13/cobra"
	"gopkg.in/yaml.v2"
)

//go:embed res.yaml
var res []byte

type command struct {
	cobra.Command
	client *managerclient.Client

	cluster  string
	all      bool
	status   string
	taskType string
	sortKey  string
}

func NewCommand(client *managerclient.Client) *cobra.Command {
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
	w.Unwrap().StringVarP(&cmd.status, "status", "s", "", "")
	w.Unwrap().StringVarP(&cmd.taskType, "type", "t", "", "")
	w.Unwrap().StringVar(&cmd.sortKey, "sort", "", "")
}

func (cmd *command) run() error {
	if err := validateSortKey(cmd.sortKey); err != nil {
		return err
	}

	var clusters []*managerclient.Cluster
	if cmd.cluster == "" {
		var err error
		clusters, err = cmd.client.ListClusters(cmd.Context())
		if err != nil {
			return err
		}
	} else {
		clusters = []*managerclient.Cluster{{ID: cmd.cluster}}
	}

	w := cmd.OutOrStdout()
	h := func(clusterID string) error {
		tasks, err := cmd.client.ListTasks(cmd.Context(), clusterID, cmd.taskType, cmd.all, cmd.status)
		if err != nil {
			return err
		}
		sortTasks(tasks, taskListSortKey(cmd.sortKey))
		return tasks.Render(w)
	}
	for _, c := range clusters {
		if cmd.cluster == "" {
			managerclient.FormatClusterName(w, c)
		}
		if err := h(c.ID); err != nil {
			managerclient.PrintError(w, err)
		}
	}

	return nil
}

type taskListSortKey string

const (
	taskListSortStartTime      taskListSortKey = "start-time"
	taskListSortEndTime        taskListSortKey = "end-time"
	taskListSortNextActivation taskListSortKey = "next-activation"
	taskListSortStatus         taskListSortKey = "status"
)

var allTaskSortKeys = []taskListSortKey{taskListSortStartTime, taskListSortNextActivation, taskListSortEndTime, taskListSortStatus}

var tasksSortFunctions = map[taskListSortKey]func(tasks managerclient.ExtendedTaskSlice){
	taskListSortStartTime:      sortTasksByStartTime,
	taskListSortEndTime:        sortTasksByEndTime,
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

func sortTasks(tasks managerclient.ExtendedTasks, key taskListSortKey) {
	if key == "" {
		return
	}
	tasksSortFunctions[key](tasks.ExtendedTaskSlice)
}

func timeLessFunc(lhvDate, rhvDate strfmt.DateTime) bool {
	lhv := timeutc.MustParse(time.RFC3339, lhvDate.String())
	rhv := timeutc.MustParse(time.RFC3339, rhvDate.String())
	return lhv.Before(rhv)
}

func sortTasksByNextActivation(tasks managerclient.ExtendedTaskSlice) {
	sort.Slice(tasks, func(i, j int) bool {
		return timeLessFunc(tasks[i].NextActivation, tasks[j].NextActivation)
	})
}

func sortTasksByStartTime(tasks managerclient.ExtendedTaskSlice) {
	sort.Slice(tasks, func(i, j int) bool {
		return timeLessFunc(tasks[i].StartTime, tasks[j].StartTime)
	})
}

func sortTasksByEndTime(tasks managerclient.ExtendedTaskSlice) {
	sort.Slice(tasks, func(i, j int) bool {
		return timeLessFunc(tasks[i].EndTime, tasks[j].EndTime)
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

func sortTasksByStatus(tasks managerclient.ExtendedTaskSlice) {
	sort.Slice(tasks, func(i, j int) bool {
		return taskStatusSortOrder[tasks[i].Status] < taskStatusSortOrder[tasks[j].Status]
	})
}
