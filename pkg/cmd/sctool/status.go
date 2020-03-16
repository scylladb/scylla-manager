// Copyright (C) 2017 ScyllaDB

package main

import (
	"fmt"

	"github.com/scylladb/mermaid/pkg/mermaidclient"
	"github.com/spf13/cobra"
)

var statusCmd = &cobra.Command{
	Use:   "status",
	Short: "Shows cluster status",

	RunE: func(cmd *cobra.Command, args []string) error {
		var clusters []*mermaidclient.Cluster
		if cfgCluster == "" {
			var err error
			if clusters, err = client.ListClusters(ctx); err != nil {
				return err
			}
		} else {
			clusters = []*mermaidclient.Cluster{{ID: cfgCluster}}
		}

		w := cmd.OutOrStdout()
		for _, c := range clusters {
			if cfgCluster == "" {
				fmt.Fprint(w, "Cluster: ")
				if c.Name != "" {
					fmt.Fprintln(w, c.Name)
				} else {
					fmt.Fprintln(w, c.ID)
				}
			}
			status, err := client.ClusterStatus(ctx, c.ID)
			if err != nil {
				return err
			}

			if err := render(w, status); err != nil {
				return err
			}
		}

		return nil
	},
}

func init() {
	cmd := statusCmd
	withScyllaDocs(cmd, "/sctool/#status")
	register(cmd, rootCmd)
}
