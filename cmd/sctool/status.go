// Copyright (C) 2017 ScyllaDB

package main

import "github.com/spf13/cobra"

var statusCmd = &cobra.Command{
	Use:   "status",
	Short: "Show cluster status",

	RunE: func(cmd *cobra.Command, args []string) error {
		w := cmd.OutOrStdout()

		status, err := client.ClusterStatus(ctx, cfgCluster)
		if err != nil {
			return printableError{err}
		}

		return render(w, status)
	},
}

func init() {
	cmd := statusCmd
	withScyllaDocs(cmd, "/sctool/#status")
	register(cmd, rootCmd)
}
