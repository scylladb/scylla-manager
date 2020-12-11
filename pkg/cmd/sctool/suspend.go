// Copyright (C) 2017 ScyllaDB

package main

import (
	"github.com/spf13/cobra"
)

var suspendCmd = &cobra.Command{
	Use:   "suspend",
	Short: "Stop execution of all tasks that are running on a cluster",
	RunE: func(cmd *cobra.Command, args []string) error {
		return client.Suspend(ctx, cfgCluster)
	},
}

func init() {
	cmd := suspendCmd
	register(cmd, rootCmd)
}

var resumeCmd = &cobra.Command{
	Use:   "resume",
	Short: "Undo suspend",
	RunE: func(cmd *cobra.Command, args []string) error {
		st, err := cmd.Flags().GetBool("start-tasks")
		if err != nil {
			return err
		}
		return client.Resume(ctx, cfgCluster, st)
	},
}

func init() {
	cmd := resumeCmd
	fs := cmd.Flags()
	fs.Bool("start-tasks", false, "start tasks that were stopped by the suspend command")
	register(cmd, rootCmd)
}
