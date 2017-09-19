// Copyright (C) 2017 ScyllaDB

package main

import (
	"os"

	"github.com/mitchellh/cli"
	"github.com/scylladb/mermaid/command"
)

// commands is the mapping of all the available commands.
func commands() map[string]cli.CommandFactory {
	ui := &cli.BasicUi{Writer: os.Stdout, ErrorWriter: os.Stderr}

	return map[string]cli.CommandFactory{
		"server": func() (cli.Command, error) {
			cmd := &command.ServerCommand{
				BaseCommand: command.BaseCommand{
					UI: ui,
				},
			}
			cmd.InitFlags()

			return cmd, nil
		},
	}
}
