// Copyright (C) 2017 ScyllaDB

package main

import (
	"fmt"
	"strings"

	"github.com/spf13/cobra"
)

func withoutArgs(cmd *cobra.Command) *cobra.Command {
	cmd.Args = cobra.NoArgs
	return cmd
}

func subcommand(cmd *cobra.Command, parent *cobra.Command) {
	parent.AddCommand(cmd)
}

func requireFlags(cmd *cobra.Command, flags ...string) {
	for _, f := range flags {
		cmd.MarkFlagRequired(f)
	}
}

func taskSplit(s string) (taskType, taskID string) {
	i := strings.LastIndex(s, "/")
	return s[:i], s[i+1:]
}

func taskJoin(taskType, taskID string) string {
	return fmt.Sprint(taskType, "/", taskID)
}
