// Copyright (C) 2017 ScyllaDB

package client

import (
	"context"
	"os"

	"github.com/mitchellh/cli"
	"github.com/scylladb/mermaid/command"
)

// DefaultAPIHost is a default TCP address of scylla-mgmt server.
var DefaultAPIHost = "localhost:9090"

// Commands is the mapping of all the available commands.
func Commands(ctx context.Context) map[string]cli.CommandFactory {
	ui := &cli.BasicUi{Writer: os.Stdout, ErrorWriter: os.Stderr}

	apiHost := os.Getenv("SCTOOL_API_HOST")
	if apiHost == "" {
		apiHost = DefaultAPIHost
	}

	cluster := os.Getenv("SCTOOL_CLUSTER")

	base := BaseClientCommand{
		BaseCommand: command.BaseCommand{
			UI: ui,
		},
		context: ctx,
		apiHost: apiHost,
		cluster: cluster,
	}

	return map[string]cli.CommandFactory{
		"repair unit list": func() (cli.Command, error) {
			cmd := &RepairUnitList{
				BaseClientCommand: base,
			}
			cmd.InitFlags()

			return cmd, nil
		},

		"repair start": func() (cli.Command, error) {
			cmd := &RepairStart{
				BaseClientCommand: base,
			}
			cmd.InitFlags()

			return cmd, nil
		},
	}
}
