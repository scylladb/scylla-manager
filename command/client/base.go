// Copyright (C) 2017 ScyllaDB

package client

import (
	"context"

	"github.com/pkg/errors"
	"github.com/scylladb/mermaid/command"
	"github.com/scylladb/mermaid/restapiclient"
	"github.com/scylladb/mermaid/restapiclient/client/operations"
)

// BaseClientCommand handles common CLI client behaviours.
type BaseClientCommand struct {
	command.BaseCommand

	context context.Context
	apiHost string
	cluster string
}

// InitFlags sets the command flags.
func (cmd *BaseClientCommand) InitFlags() {
	f := cmd.NewFlagSet(cmd.Help)
	f.StringVar(&cmd.cluster, "cluster", cmd.cluster, "ID or name of a cluster.")
}

func (cmd *BaseClientCommand) validate() error {
	if cmd.cluster == "" {
		return errors.New("missing cluster")
	}

	return nil
}

func (cmd *BaseClientCommand) client() *operations.Client {
	return restapiclient.New(cmd.apiHost)
}
