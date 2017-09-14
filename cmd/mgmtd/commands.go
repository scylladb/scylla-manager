// Copyright (C) 2017 ScyllaDB

package main

import (
	"github.com/mitchellh/cli"
	"github.com/scylladb/mermaid/command"
)

// commands is the mapping of all the available commands.
func commands() map[string]cli.CommandFactory {
	return map[string]cli.CommandFactory{
		"server": func() (cli.Command, error) {
			return &command.ServerCommand{}, nil
		},
	}
}
