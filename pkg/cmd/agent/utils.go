// Copyright (C) 2017 ScyllaDB

package main

import "github.com/spf13/cobra"

func requireFlags(cmd *cobra.Command, flags ...string) {
	for _, f := range flags {
		if err := cmd.MarkFlagRequired(f); err != nil {
			panic(err)
		}
	}
}
