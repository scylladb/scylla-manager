// Copyright (C) 2017 ScyllaDB

package clusterlist

import (
	_ "embed"

	"github.com/scylladb/scylla-manager/pkg/managerclient"
	"github.com/spf13/cobra"
	"gopkg.in/yaml.v2"
)

//go:embed res.yaml
var res []byte

type command struct {
	cobra.Command
	client *managerclient.Client
}

func NewCommand(client *managerclient.Client) *cobra.Command {
	cmd := &command{
		client: client,
	}
	if err := yaml.Unmarshal(res, &cmd.Command); err != nil {
		panic(err)
	}
	cmd.RunE = func(_ *cobra.Command, args []string) error {
		return cmd.run()
	}
	return &cmd.Command
}

func (cmd *command) run() error {
	clusters, err := cmd.client.ListClusters(cmd.Context())
	if err != nil {
		return err
	}
	return clusters.Render(cmd.OutOrStdout())
}
