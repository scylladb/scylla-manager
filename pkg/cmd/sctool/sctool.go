// Copyright (C) 2017 ScyllaDB

package main

import (
	"io"
	"log"
	"os"

	"github.com/scylladb/scylla-manager/v3/pkg/command/backup"
	"github.com/scylladb/scylla-manager/v3/pkg/command/backup/backupdelete"
	"github.com/scylladb/scylla-manager/v3/pkg/command/backup/backupfiles"
	"github.com/scylladb/scylla-manager/v3/pkg/command/backup/backuplist"
	"github.com/scylladb/scylla-manager/v3/pkg/command/backup/backupvalidate"
	"github.com/scylladb/scylla-manager/v3/pkg/command/cluster/clusteradd"
	"github.com/scylladb/scylla-manager/v3/pkg/command/cluster/clusterdelete"
	"github.com/scylladb/scylla-manager/v3/pkg/command/cluster/clusterlist"
	"github.com/scylladb/scylla-manager/v3/pkg/command/cluster/clusterupdate"
	"github.com/scylladb/scylla-manager/v3/pkg/command/info"
	"github.com/scylladb/scylla-manager/v3/pkg/command/legacy/task/taskdelete"
	"github.com/scylladb/scylla-manager/v3/pkg/command/legacy/task/taskhistory"
	"github.com/scylladb/scylla-manager/v3/pkg/command/legacy/task/tasklist"
	"github.com/scylladb/scylla-manager/v3/pkg/command/legacy/task/taskprogress"
	"github.com/scylladb/scylla-manager/v3/pkg/command/legacy/task/taskstart"
	"github.com/scylladb/scylla-manager/v3/pkg/command/legacy/task/taskstop"
	"github.com/scylladb/scylla-manager/v3/pkg/command/legacy/task/taskupdate"
	"github.com/scylladb/scylla-manager/v3/pkg/command/progress"
	"github.com/scylladb/scylla-manager/v3/pkg/command/repair"
	"github.com/scylladb/scylla-manager/v3/pkg/command/repair/repaircontrol"
	"github.com/scylladb/scylla-manager/v3/pkg/command/restore"
	"github.com/scylladb/scylla-manager/v3/pkg/command/resume"
	"github.com/scylladb/scylla-manager/v3/pkg/command/start"
	"github.com/scylladb/scylla-manager/v3/pkg/command/status"
	"github.com/scylladb/scylla-manager/v3/pkg/command/stop"
	"github.com/scylladb/scylla-manager/v3/pkg/command/suspend"
	"github.com/scylladb/scylla-manager/v3/pkg/command/tasks"
	"github.com/scylladb/scylla-manager/v3/pkg/command/version"
	managerclient2 "github.com/scylladb/scylla-manager/v3/pkg/managerclient"
	"github.com/spf13/cobra"
)

func main() {
	log.SetOutput(io.Discard)

	cmd := buildCommand()
	if err := cmd.Execute(); err != nil {
		managerclient2.PrintError(cmd.OutOrStderr(), err)
		os.Exit(1)
	}

	os.Exit(0)
}

func buildCommand() *cobra.Command {
	var client managerclient2.Client

	backupCmd := backup.NewCommand(&client)
	backupCmd.AddCommand(
		backupdelete.NewCommand(&client),
		backupfiles.NewCommand(&client),
		backuplist.NewCommand(&client),
		backupvalidate.NewCommand(&client),
	)

	restoreCmd := restore.NewCommand(&client)

	clusterCmd := &cobra.Command{
		Use:   "cluster",
		Short: "Add or delete clusters",
	}
	clusterCmd.AddCommand(
		clusteradd.NewCommand(&client),
		clusterdelete.NewCommand(&client),
		clusterlist.NewCommand(&client),
		clusterupdate.NewCommand(&client),
	)

	repairCmd := repair.NewCommand(&client)
	repairCmd.AddCommand(repaircontrol.NewCommand(&client))

	taskCmd := &cobra.Command{
		Use:        "task",
		Short:      "Start, stop and track task progress",
		Deprecated: "see subcommands for details.",
	}
	taskCmd.AddCommand(
		taskdelete.NewCommand(&client),
		taskhistory.NewCommand(&client),
		tasklist.NewCommand(&client),
		taskprogress.NewCommand(&client),
		taskstart.NewCommand(&client),
		taskstop.NewCommand(&client),
		taskupdate.NewCommand(&client),
	)

	rootCmd := newRootCommand(&client)
	rootCmd.AddCommand(
		backupCmd,
		restoreCmd,
		clusterCmd,
		info.NewCommand(&client),
		repairCmd,
		resume.NewCommand(&client),
		progress.NewCommand(&client),
		start.NewCommand(&client),
		status.NewCommand(&client),
		stop.NewCommand(&client),
		suspend.NewCommand(&client),
		taskCmd,
		tasks.NewCommand(&client),
		version.NewCommand(&client),
	)
	setCommandDefaults(rootCmd)
	addCompletionCommand(rootCmd)
	addDocCommand(rootCmd)

	return rootCmd
}

func setCommandDefaults(cmd *cobra.Command) {
	// By default do not accept any arguments
	if cmd.Args == nil {
		cmd.Args = cobra.NoArgs
	}
	// Do not print errors, error printing is handled in main
	cmd.SilenceErrors = true
	cmd.SilenceUsage = true

	// Call recursively.
	for _, c := range cmd.Commands() {
		setCommandDefaults(c)
	}
}
