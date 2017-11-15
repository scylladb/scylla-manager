// Copyright (C) 2017 ScyllaDB

package main

import "github.com/spf13/cobra"

var bashCompletionCmd = &cobra.Command{
	Use:    "_bashcompletion",
	Hidden: true,
	Run: func(cmd *cobra.Command, args []string) {
		rootCmd.GenBashCompletion(cmd.OutOrStdout())
	},
}

func init() {
	rootCmd.AddCommand(bashCompletionCmd)
}
