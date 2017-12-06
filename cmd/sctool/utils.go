// Copyright (C) 2017 ScyllaDB

package main

import "github.com/spf13/cobra"

func withoutArgs(cmd *cobra.Command) *cobra.Command {
	cmd.Args = cobra.NoArgs
	return cmd
}

func subcommand(cmd *cobra.Command, parent *cobra.Command) {
	parent.AddCommand(cmd)
}

func require(cmd *cobra.Command, flag string) {
	cmd.MarkFlagRequired(flag)
}
