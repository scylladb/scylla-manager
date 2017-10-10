// Copyright (C) 2017 ScyllaDB

package server

import (
	"os"

	"github.com/mitchellh/cli"
	"github.com/scylladb/mermaid/command"
)

// Commands is the mapping of all the available commands.
func Commands() map[string]cli.CommandFactory {
	ui := &cli.BasicUi{Writer: os.Stdout, ErrorWriter: os.Stderr}

	return map[string]cli.CommandFactory{
		"server": func() (cli.Command, error) {
			cmd := &Command{
				BaseCommand: command.BaseCommand{
					UI: ui,
				},
			}
			cmd.InitFlags()

			return cmd, nil
		},
	}
}
