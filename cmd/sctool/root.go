// Copyright (C) 2017 ScyllaDB

package main

import (
	"os"

	"github.com/pkg/errors"
	"github.com/scylladb/mermaid/mermaidclient"
	"github.com/spf13/cobra"
)

var (
	defaultURL = "http://localhost:9090/api/v1"

	cfgURL     string
	cfgCluster string

	client *mermaidclient.Client
)

var rootCmd = &cobra.Command{
	Use:   "sctool",
	Short: "Scylla management client",

	PersistentPreRunE: func(cmd *cobra.Command, args []string) error {
		if cmd.IsAdditionalHelpTopicCommand() {
			return nil
		}

		// cluster
		if cfgCluster == "" {
			return errors.New("missing cluster")
		}

		// init client
		if err := initClient(); err != nil {
			return errors.Wrap(err, "failed to init client")
		}

		return nil
	},
}

func initClient() error {
	c, err := mermaidclient.NewClient(cfgURL, cfgCluster)
	if err != nil {
		return err
	}

	client = c

	return nil
}

func init() {
	url := os.Getenv("SCYLLA_MGMT_API_URL")
	if url == "" {
		url = defaultURL
	}
	rootCmd.PersistentFlags().StringVar(&cfgURL, "api-url", url, "`URL` of Scylla management server")
	rootCmd.PersistentFlags().StringVarP(&cfgCluster, "cluster", "c", os.Getenv("SCYLLA_MGMT_CLUSTER"), "target cluster `name` or ID")
}
