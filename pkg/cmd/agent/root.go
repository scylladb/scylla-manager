// Copyright (C) 2017 ScyllaDB

package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/pkg/errors"
	"github.com/scylladb/go-log"
	"github.com/scylladb/scylla-manager/pkg"
	"github.com/scylladb/scylla-manager/pkg/config"
	"github.com/spf13/cobra"
)

var rootArgs = struct {
	configFiles []string
	version     bool
}{}

var rootCmd = &cobra.Command{
	Use:           "scylla-manager",
	Short:         "Scylla Manager agent",
	Args:          cobra.NoArgs,
	SilenceUsage:  true,
	SilenceErrors: true,

	RunE: func(cmd *cobra.Command, args []string) (runError error) {
		// Print version and return
		if rootArgs.version {
			fmt.Fprintf(cmd.OutOrStdout(), "%s\n", pkg.Version())
			return nil
		}

		c, err := config.ParseAgentConfigFiles(rootArgs.configFiles)
		if err != nil {
			return err
		}
		if err := c.Validate(); err != nil {
			return errors.Wrap(err, "invalid config")
		}

		// Get a base context with tracing id
		ctx := log.WithNewTraceID(context.Background())

		logger, err := config.MakeLogger(c.Logger)
		if err != nil {
			return errors.Wrapf(err, "logger")
		}
		defer func() {
			if runError != nil {
				logger.Error(ctx, "Bye", "error", runError)
			} else {
				logger.Info(ctx, "Bye")
			}
			logger.Sync() // nolint
		}()

		// Start server
		s := newServer(c, logger)
		if err := s.init(ctx); err != nil {
			return errors.Wrapf(err, "server init")
		}
		if err := s.makeServers(ctx); err != nil {
			return errors.Wrapf(err, "make servers")
		}
		s.startServers(ctx)
		defer s.shutdownServers(ctx, 30*time.Second)

		// Wait signal
		signalCh := make(chan os.Signal, 1)
		signal.Notify(signalCh, syscall.SIGINT, syscall.SIGTERM)
		select {
		case err := <-s.errCh:
			if err != nil {
				return err
			}
		case sig := <-signalCh:
			logger.Info(ctx, "Received signal", "signal", sig)
		}

		return nil
	},
}

func init() {
	f := rootCmd.Flags()
	f.StringSliceVarP(&rootArgs.configFiles, "config-file", "c", []string{"/etc/scylla-manager-agent/scylla-manager-agent.yaml"}, "configuration file `path`")
	f.BoolVar(&rootArgs.version, "version", false, "print version and exit")
}
