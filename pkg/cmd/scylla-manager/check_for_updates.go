// Copyright (C) 2017 ScyllaDB

package main

import (
	"context"
	"fmt"

	"github.com/pkg/errors"
	"github.com/scylladb/mermaid/pkg/callhome"
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

		return checkForUpdate(context.Background(), cmd, install)
	},
}

func init() {
	rootCmd.AddCommand(checkForUpdatesCmd)

	fs := checkForUpdatesCmd.Flags()
	fs.Bool("install", false, "use installation status when performing check")
	fs.MarkHidden("install") // nolint:errcheck
}

func checkForUpdate(ctx context.Context, cmd *cobra.Command, install bool) error {
	ch := callhome.NewChecker("", "", callhome.DefaultEnv)
	res, err := ch.CheckForUpdates(ctx, install)
	if err != nil {
		return errors.Wrap(err, "check for updates")
	}

	if res.UpdateAvailable {
		msg := "New Scylla Manager version is available"
		fmt.Fprintf(cmd.OutOrStdout(), msg+": installed=%s available=%s\n", res.Installed, res.Available)
	} else {
		msg := "Scylla Manager is up to date"
		fmt.Fprintf(cmd.OutOrStdout(), msg+": installed=%s\n", res.Installed)
	}
	return nil
}
