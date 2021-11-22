// Copyright (C) 2017 ScyllaDB

package main

import (
	"github.com/pkg/errors"
	"github.com/spf13/cobra"
)

type shell string

const (
	bash shell = "bash"
	fish shell = "fish"
	zsh  shell = "zsh"
)

var allShells = []shell{
	bash,
	fish,
	zsh,
}

func completion(cmd *cobra.Command, s shell) error {
	switch s {
	case bash:
		return cmd.GenBashCompletion(cmd.OutOrStdout())
	case fish:
		return cmd.GenFishCompletion(cmd.OutOrStdout(), true)
	case zsh:
		return cmd.GenZshCompletion(cmd.OutOrStdout())
	}

	return errors.Errorf("unsupported shell %q", s)
}

func addCompletionCommand(rootCmd *cobra.Command) {
	cmd := &cobra.Command{
		Use:   "completion",
		Short: "Generate shell completion",
	}
	for _, s := range allShells {
		cmd.AddCommand(&cobra.Command{
			Use:   string(s),
			Short: "Generate " + string(s) + "completion",
			RunE: func(cmd *cobra.Command, args []string) error {
				return completion(rootCmd, s)
			},
		})
	}
	rootCmd.AddCommand(cmd)
}
