// Copyright (C) 2017 ScyllaDB

package main

import (
	"fmt"

	"github.com/scylladb/mermaid"
	"github.com/spf13/cobra"
)

var versionCmd = withoutArgs(&cobra.Command{
	Use:   "version",
	Short: "Show version information",
	Run: func(cmd *cobra.Command, args []string) {
		fmt.Fprintln(cmd.OutOrStdout(), mermaid.Version())
	},
})

func init() {
	rootCmd.AddCommand(versionCmd)
}
