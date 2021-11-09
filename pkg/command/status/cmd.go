// Copyright (C) 2017 ScyllaDB

package status

import (
	_ "embed"
	"fmt"

	"github.com/scylladb/scylla-manager/pkg"
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

	cluster string
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
}

func (cmd *command) run() error {
	fmt.Fprintf(cmd.OutOrStdout(), "Client version: %v\n", pkg.Version())

	var clusters []*managerclient.Cluster
	if cmd.cluster == "" {
		var err error
		if clusters, err = cmd.client.ListClusters(cmd.Context()); err != nil {
			return err
		}
	} else {
		clusters = []*managerclient.Cluster{{ID: cmd.cluster}}
	}

	w := cmd.OutOrStdout()
	h := func(clusterID string) error {
		status, err := cmd.client.ClusterStatus(cmd.Context(), clusterID)
		if err != nil {
			return err
		}
		return status.Render(w)
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
