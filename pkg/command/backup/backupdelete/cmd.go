// Copyright (C) 2017 ScyllaDB

package backupdelete

import (
	_ "embed"
	"fmt"
	"time"

	"github.com/scylladb/scylla-manager/pkg/command/flag"
	"github.com/scylladb/scylla-manager/pkg/managerclient"
	"github.com/spf13/cobra"
	"go.uber.org/atomic"
	"gopkg.in/yaml.v2"
)

//go:embed res.yaml
var res []byte

type command struct {
	cobra.Command
	client *managerclient.Client

	cluster     string
	location    []string
	snapshotTag []string
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
	defer flag.MustSetUsages(&cmd.Command, res, "cluster", "snapshot-tag")

	w := flag.Wrap(cmd.Flags())
	w.Cluster(&cmd.cluster)
	w.Location(&cmd.location)
	w.Unwrap().StringSliceVarP(&cmd.snapshotTag, "snapshot-tag", "T", nil, "")
}

func (cmd *command) run() error {
	stillWaiting := atomic.NewBool(true)
	time.AfterFunc(5*time.Second, func() {
		if stillWaiting.Load() {
			fmt.Fprintf(cmd.OutOrStderr(), "NOTICE: this may take a while, we are reading metadata from backup location(s)\n")
		}
	})
	return cmd.client.DeleteSnapshot(cmd.Context(), cmd.cluster, cmd.location, cmd.snapshotTag)
}
