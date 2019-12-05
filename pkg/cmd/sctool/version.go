// Copyright (C) 2017 ScyllaDB

package main

import (
	"fmt"

	"github.com/scylladb/mermaid/pkg"
	"github.com/spf13/cobra"
)

var versionCmd = &cobra.Command{
	Use:   "version",
	Short: "Shows version information",
	RunE: func(cmd *cobra.Command, args []string) error {
		fmt.Fprintln(cmd.OutOrStdout(), fmt.Sprintf("Client version: %v", pkg.Version()))

		sv, err := client.Version(ctx)
		if err != nil {
			return err
		}

		fmt.Fprintln(cmd.OutOrStdout(), fmt.Sprintf("Server version: %v", sv.Version))

		return nil
	},
}

func init() {
	register(versionCmd, rootCmd)
}
