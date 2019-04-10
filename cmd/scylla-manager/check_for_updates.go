// Copyright (C) 2017 ScyllaDB

package main

import (
	"context"

	"github.com/scylladb/mermaid/callhome"
	"github.com/spf13/cobra"
)

var checkForUpdatesCmd = &cobra.Command{
	Use:   "check-for-updates",
	Short: "Check for updates",
	Args:  cobra.NoArgs,

	RunE: func(cmd *cobra.Command, args []string) error {
		install, err := cmd.Flags().GetBool("install")
		if err != nil {
			return err
		}

		// Reuse configuration loading from root to make sure we are using
		// same logger configuration.
		config, err := newConfigFromFile(cfgConfigFile...)
		if err != nil {
			config = defaultConfig()
		}
		if err := config.validate(); err != nil {
			config = defaultConfig()
		}

		l, err := logger(config)
		if err != nil {
			return err
		}

		ch := callhome.NewChecker("", "", l.Named("callhome"), callhome.DefaultEnv)
		return ch.CheckForUpdates(context.Background(), install)
	},
}

func init() {
	rootCmd.AddCommand(checkForUpdatesCmd)

	fs := checkForUpdatesCmd.Flags()
	fs.Bool("install", false, "use installation status when performing check")
	fs.MarkHidden("install") // nolint:errcheck
}
