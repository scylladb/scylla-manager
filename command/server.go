// Copyright (C) 2017 ScyllaDB

package command

import (
	"context"
	"fmt"
	"os"

	"github.com/scylladb/mermaid/log"
	"go.uber.org/zap"
)

// ServerCommand is a Command implementation prints the version.
type ServerCommand struct {
}

// Help implements cli.Command.
func (c *ServerCommand) Help() string {
	return ""
}

// Run implements cli.Command.
func (c *ServerCommand) Run(_ []string) int {
	z, err := zap.NewProduction()
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to init logger: %s", err)
		return 1
	}
	logger := log.NewLogger(z)

	logger.Info(context.Background(), "Server started")
	logger.Info(context.Background(), "Server stopped")

	return 0
}

// Synopsis implements cli.Command.
func (c *ServerCommand) Synopsis() string {
	return "Starts the Scylla management server"
}
