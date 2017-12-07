// Copyright (C) 2017 ScyllaDB

package main

import (
	"os"

	"github.com/scylladb/mermaid/mermaidclient"
	"github.com/spf13/cobra"
)

var (
	defaultURL = "http://localhost:9090/api/v1"

	cfgURL     string
	cfgCluster string

	client mermaidclient.Client
)

var rootCmd = &cobra.Command{
	Use:   "sctool",
	Short: "Scylla management client",

	PersistentPreRunE: func(cmd *cobra.Command, args []string) error {
		if cmd.IsAdditionalHelpTopicCommand() || cmd.Hidden {
			return nil
		}

		// init client
		c, err := mermaidclient.NewClient(cfgURL)
		if err != nil {
			return err
		}
		client = c

		// requireFlags cluster
		if needsCluster(cmd) {
			cluster := os.Getenv("SCYLLA_MGMT_CLUSTER")

			cmd.Flags().StringVarP(&cfgCluster, "cluster", "c", cluster, "target cluster `name` or ID")
			if cluster == "" {
				cmd.MarkFlagRequired("cluster")
			}
		}

		return nil
	},
}

func needsCluster(cmd *cobra.Command) bool {
	return cmd != clusterAddCmd && cmd != versionCmd
}

func init() {
	url := os.Getenv("SCYLLA_MGMT_API_URL")
	if url == "" {
		url = defaultURL
	}
	rootCmd.PersistentFlags().StringVar(&cfgURL, "api-url", url, "`URL` of Scylla management server")
}
