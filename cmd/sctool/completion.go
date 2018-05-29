// Copyright (C) 2017 ScyllaDB

package main

import "github.com/spf13/cobra"

var bashCompletionCmd = &cobra.Command{
	Use:    "_bashcompletion",
	Hidden: true,
	RunE: func(cmd *cobra.Command, args []string) error {
		return rootCmd.GenBashCompletion(cmd.OutOrStdout())
	},
}

func init() {
	register(bashCompletionCmd, rootCmd)
}
