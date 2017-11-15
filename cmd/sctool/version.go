// Copyright (C) 2017 ScyllaDB

package main

import (
	"fmt"

	"github.com/spf13/cobra"
)

var version = "Snapshot"

var versionCmd = &cobra.Command{
	Use:   "version",
	Short: "Show version information",
	Run: func(cmd *cobra.Command, args []string) {
		fmt.Fprintln(cmd.OutOrStdout(), version)
	},
}

func init() {
	rootCmd.AddCommand(versionCmd)
}
