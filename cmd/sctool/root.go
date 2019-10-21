// Copyright (C) 2017 ScyllaDB

package main

import (
	"crypto/tls"
	"os"

	"github.com/pkg/errors"
	"github.com/scylladb/mermaid/mermaidclient"
	"github.com/spf13/cobra"
)

var (
	defaultURL = "https://127.0.0.1:56443/api/v1"

	cfgAPIURL      string
	cfgAPICertFile string
	cfgAPIKeyFile  string
	cfgCluster     string

	client mermaidclient.Client
)

var rootCmd = &cobra.Command{
	Use:   "sctool",
	Short: "Scylla Manager " + docsVersion,

	PersistentPreRunE: func(cmd *cobra.Command, args []string) error {
		if cmd.IsAdditionalHelpTopicCommand() || cmd.Hidden {
			return nil
		}

		// Init client
		tlsConfig := mermaidclient.DefaultTLSConfig()

		// Load TLS certificate if provided
		if cfgAPICertFile != "" && cfgAPIKeyFile == "" {
			return errors.New("missing flag \"api-key-file\"")
		}
		if cfgAPIKeyFile != "" && cfgAPICertFile == "" {
			return errors.New("missing flag \"api-cert-file\"")
		}
		if cfgAPICertFile != "" {
			cert, err := tls.LoadX509KeyPair(cfgAPICertFile, cfgAPIKeyFile)
			if err != nil {
				return errors.Wrap(err, "load client certificate")
			}
			tlsConfig.Certificates = []tls.Certificate{cert}
		}

		c, err := mermaidclient.NewClient(cfgAPIURL, tlsConfig)
		if err != nil {
			return err
		}
		client = c

		// RequireFlags cluster
		if needsCluster(cmd) {
			if os.Getenv("SCYLLA_MANAGER_CLUSTER") == "" {
				if err := cmd.Root().MarkFlagRequired("cluster"); err != nil {
					return err
				}
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
	f := rootCmd.PersistentFlags()

	url := os.Getenv("SCYLLA_MANAGER_API_URL")
	if url == "" {
		url = defaultURL
	}
	f.StringVar(&cfgAPIURL, "api-url", url, "`URL` of Scylla Manager server")
	f.StringVar(&cfgAPICertFile, "api-cert-file", os.Getenv("SCYLLA_MANAGER_API_CERT_FILE"), "`path` to HTTPS client certificate to access Scylla Manager server")
	f.StringVar(&cfgAPIKeyFile, "api-key-file", os.Getenv("SCYLLA_MANAGER_API_KEY_FILE"), "`path` to HTTPS client key to access Scylla Manager server")

	f.StringVarP(&cfgCluster, "cluster", "c", os.Getenv("SCYLLA_MANAGER_CLUSTER"), "target cluster `name` or ID")
}
