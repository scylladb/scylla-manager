// Copyright (C) 2017 ScyllaDB

package main

import (
	"fmt"
	"net/http"
	"os"

	"github.com/scylladb/mermaid"
	"github.com/scylladb/mermaid/rclone/rcserver"
	"github.com/spf13/cobra"
)

func main() {
	if err := rootCmd.Execute(); err != nil {
		fmt.Fprint(rootCmd.OutOrStderr(), err)
		os.Exit(1)
	}

	os.Exit(0)
}

var rootArgs = struct {
	configFile       string
	scyllaConfigFile string
	version          bool
}{}

var rootCmd = &cobra.Command{
	Use:           "scylla-manager",
	Short:         "Scylla Manager server",
	Args:          cobra.NoArgs,
	SilenceUsage:  true,
	SilenceErrors: true,

	RunE: func(cmd *cobra.Command, args []string) error {
		// Print version and return
		if rootArgs.version {
			fmt.Fprintf(cmd.OutOrStdout(), "%s\n", mermaid.Version())
			return nil
		}

		// Parse config
		if _, err := os.Stat(rootArgs.scyllaConfigFile); os.IsNotExist(err) {
			rootArgs.scyllaConfigFile = ""
		}
		c, err := parseConfigFiles(rootArgs.configFile, rootArgs.scyllaConfigFile)
		if err != nil {
			return err
		}

		// Start server
		server := http.Server{
			Addr:    c.HTTP,
			Handler: newRouter(c, rcserver.New(), http.DefaultClient),
		}
		return server.ListenAndServe()
	},
}

func init() {
	f := rootCmd.Flags()
	f.StringVarP(&rootArgs.configFile, "config-file", "c", "/etc/scylla-manager-agent/scylla-manager-agent.yaml", "configuration file `path`")
	f.StringVar(&rootArgs.scyllaConfigFile, "scylla-config-file", "/etc/scylla/scylla.yaml", "Scylla server configuration file `path`")
	f.BoolVar(&rootArgs.version, "version", false, "print product version and exit")
}
