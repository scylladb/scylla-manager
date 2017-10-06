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

	apiHost := os.Getenv("SCTOOL_API_HOST")
	if apiHost == "" {
		apiHost = "localhost:9090"
	}

	return map[string]cli.CommandFactory{
		"repair unit list": func() (cli.Command, error) {
			cmd := &command.RepairUnitList{
				BaseClientCommand: command.BaseClientCommand{
					BaseCommand: command.BaseCommand{
						UI: ui,
					},
					APIHost: apiHost,
				},
			}
			cmd.InitFlags()

			return cmd, nil
		},
	}
}
