// Copyright (C) 2017 ScyllaDB

package main

import (
	"fmt"
	"os"

	"github.com/pkg/errors"
	"github.com/spf13/cobra"
	"github.com/spf13/cobra/doc"
)

type docCommand struct {
	cobra.Command
	output string
}

func (cmd *docCommand) init() {
	w := cmd.Flags()
	w.StringVarP(&cmd.output, "output", "o", "", "Output `dir`ectory")
}

func (cmd *docCommand) run() error {
	if cmd.output == "" {
		dir, err := os.MkdirTemp(".", "")
		if err != nil {
			return errors.Wrap(err, "create temp dir")
		}
		cmd.output = dir
	} else {
		if err := os.MkdirAll(cmd.output, os.ModePerm); err != nil {
			return err
		}
	}
	if err := doc.GenYamlTree(cmd.Parent(), cmd.output); err != nil {
		return errors.Wrap(err, "gen yaml tree")
	}
	w := cmd.OutOrStdout()
	fmt.Fprintf(w, "Generated files in %s\n", cmd.output)

	return nil
}

func addDocCommand(rootCmd *cobra.Command) {
	cmd := &docCommand{}
	cmd.Command = cobra.Command{
		Use:    "doc",
		Hidden: true,
		RunE: func(_ *cobra.Command, _ []string) error {
			return cmd.run()
		},
	}
	cmd.init()

	rootCmd.AddCommand(&cmd.Command)
}
