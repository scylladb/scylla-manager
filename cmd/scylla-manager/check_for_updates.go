// Copyright (C) 2017 ScyllaDB

package main

import (
	"context"
	"fmt"

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

		// Reuse configuration loading from root.go to make sure we are using
		// same logger configuration.
		config, err := parseConfigFile(cfgConfigFile...)
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

		ch := callhome.NewChecker("", "", callhome.DefaultEnv)
		res, err := ch.CheckForUpdates(context.Background(), install)
		if err != nil {
			return err
		}
		msg := "New Scylla Manager version is available"
		if res.UpdateAvailable {
			if install {
				fmt.Fprintf(cmd.OutOrStderr(), msg+": installed=%s available=%s", res.Installed, res.Available)
			} else {
				l.Info(context.Background(), msg,
					"installed", res.Installed, "available", res.Available)
			}

		}
		return nil
	},
}

func init() {
	rootCmd.AddCommand(checkForUpdatesCmd)

	fs := checkForUpdatesCmd.Flags()
	fs.Bool("install", false, "use installation status when performing check")
	fs.MarkHidden("install") // nolint:errcheck
}
