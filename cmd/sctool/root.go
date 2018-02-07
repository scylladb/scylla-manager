// Copyright (C) 2017 ScyllaDB

package main

import (
	"os"

	"github.com/scylladb/mermaid/mermaidclient"
	"github.com/spf13/cobra"
)

var (
	defaultURL = "http://localhost:8889/api/v1"

	cfgURL     string
	cfgCluster string

	client mermaidclient.Client
)

var rootCmd = &cobra.Command{
	Use:   "sctool",
	Short: "Scylla Manager client",

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
			if os.Getenv("SCYLLA_MANAGER_CLUSTER") == "" {
				cmd.Root().MarkFlagRequired("cluster")
			}
		}

		return nil
	},
}

func needsCluster(cmd *cobra.Command) bool {
	switch cmd {
	case clusterAddCmd, clusterListCmd, taskListCmd, versionCmd:
		return false
	}
	return true
}

func init() {
	url := os.Getenv("SCYLLA_MANAGER_API_URL")
	if url == "" {
		url = defaultURL
	}
	rootCmd.PersistentFlags().StringVar(&cfgURL, "api-url", url, "`URL` of Scylla Manager server")

	cluster := os.Getenv("SCYLLA_MANAGER_CLUSTER")
	rootCmd.PersistentFlags().StringVarP(&cfgCluster, "cluster", "c", cluster, "target cluster `name` or ID")
}
