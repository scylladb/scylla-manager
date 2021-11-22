// Copyright (C) 2017 ScyllaDB

package main

import (
	"fmt"
	"os"

	"github.com/pkg/errors"
	"github.com/spf13/cobra"
	"github.com/spf13/cobra/doc"
)

func addDocCommand(rootCmd *cobra.Command) {
	rootCmd.AddCommand(&cobra.Command{
		Use:    "doc",
		Hidden: true,
		RunE: func(cmd *cobra.Command, args []string) error {
			dir, err := os.MkdirTemp(".", "")
			if err != nil {
				return errors.Wrap(err, "create temp dir")
			}
			if err := doc.GenYamlTree(rootCmd, dir); err != nil {
				return errors.Wrap(err, "gen yaml tree")
			}
			w := cmd.OutOrStdout()
			fmt.Fprintf(w, "Generated files in %s\n", dir)

			return nil
		},
	})
}
