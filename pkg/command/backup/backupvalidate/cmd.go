// Copyright (C) 2017 ScyllaDB

package backupvalidate

import (
	_ "embed"
	"fmt"

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

	cluster             string
	location            []string
	deleteOrphanedFiles bool
	parallel            int
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
	defer flag.MustSetUsages(&cmd.Command, res, "cluster")

	w := flag.Wrap(cmd.Flags())
	w.Cluster(&cmd.cluster)
	w.Location(&cmd.location)
	w.Unwrap().BoolVar(&cmd.deleteOrphanedFiles, "delete-orphaned-files", false, "")
	w.Unwrap().IntVar(&cmd.parallel, "parallel", 0, "")
}

func (cmd *command) run() error {
	t := &managerclient.Task{
		Type:       "validate_backup",
		Enabled:    cmd.Enabled(),
		Schedule:   cmd.Schedule(),
		Properties: make(map[string]interface{}),
	}

	props := t.Properties.(map[string]interface{})
	if len(cmd.location) != 0 {
		props["location"] = cmd.location
	}
	if cmd.deleteOrphanedFiles {
		props["delete_orphaned_files"] = true
	}
	if cmd.parallel > 0 {
		props["parallel"] = cmd.parallel
	}

	id, err := cmd.client.CreateTask(cmd.Context(), cmd.cluster, t)
	if err != nil {
		return err
	}

	fmt.Fprintln(cmd.OutOrStdout(), managerclient.TaskJoin(t.Type, id))
	return nil
}
