// Copyright (C) 2017 ScyllaDB

package main

import (
	"context"
	"fmt"
	"net"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/pkg/errors"
	"github.com/scylladb/go-log"
	"github.com/spf13/cobra"
	"go.uber.org/zap"

	"github.com/scylladb/scylla-manager/v3/pkg"
	"github.com/scylladb/scylla-manager/v3/pkg/callhome"
	config "github.com/scylladb/scylla-manager/v3/pkg/config/server"
	"github.com/scylladb/scylla-manager/v3/pkg/util/netwait"
)

var rootArgs = struct {
	configFiles []string
	version     bool
}{}

var rootCmd = &cobra.Command{
	Use:           "scylla-manager",
	Short:         "Scylla Manager server",
	Args:          cobra.NoArgs,
	SilenceUsage:  true,
	SilenceErrors: true,

	RunE: func(cmd *cobra.Command, _ []string) (runError error) {
		// Print version and return
		if rootArgs.version {
			fmt.Fprintf(cmd.OutOrStdout(), "%s\n", pkg.Version())
			return nil
		}

		// Read configuration
		c, err := config.ParseConfigFiles(rootArgs.configFiles)
		if err != nil {
			runError = errors.Wrapf(err, "configuration %q", rootArgs.configFiles)
			fmt.Fprintf(cmd.OutOrStderr(), "%s\n", runError)
			return runError
		}
		if err := c.Validate(); err != nil {
			runError = errors.Wrapf(err, "configuration %q", rootArgs.configFiles)
			fmt.Fprintf(cmd.OutOrStderr(), "%s\n", runError)
			return runError
		}

		// Create logger
		logger, err := c.MakeLogger()
		if err != nil {
			return errors.Wrapf(err, "logger")
		}
		// Get a base context
		rootCtx := log.WithNewTraceID(context.Background())
		ctx, cancel := context.WithCancel(rootCtx)
		defer func() {
			if runError != nil {
				logger.Error(ctx, "Bye", "error", runError)
			} else {
				logger.Info(ctx, "Bye")
			}
			logger.Sync() // nolint
		}()

		// Wait signal
		go func() {
			signalCh := make(chan os.Signal, 1)
			signal.Notify(signalCh, syscall.SIGINT, syscall.SIGTERM)

			sig := <-signalCh
			logger.Info(ctx, "Received signal, cancel context", "signal", sig)
			cancel()
		}()

		// Log version and check for updates
		logger.Info(ctx, "Scylla Manager Server", "version", pkg.Version(), "pid", os.Getpid())
		if pkg.Version() != "Snapshot" {
			if res, err := callhome.NewChecker("", "", callhome.DefaultEnv).CheckForUpdates(ctx, false); err != nil {
				logger.Error(ctx, "Failed to check for updates", "error", err)
			} else if res.UpdateAvailable {
				logger.Info(ctx, "New Scylla Manager version is available", "installed", res.Installed, "available", res.Available)
			}
		}
		// Log config
		logger.Info(ctx, "Using config", "c", config.Obfuscate(c), "config_files", rootArgs.configFiles)

		// Redirect standard logger to the logger
		zap.RedirectStdLog(log.BaseOf(logger))
		// Set logger to netwait
		netwait.DefaultWaiter.Logger = logger.Named("wait")
		// Wait for database
		logger.Info(ctx, "Checking database connectivity...")
		initHost, err := netwait.AnyHostPort(ctx, c.Database.Hosts, "9042")
		if err != nil {
			return errors.Wrapf(
				err,
				"no connection to database, make sure Scylla server is running and that database section in c file(s) %s is set correctly",
				strings.Join(rootArgs.configFiles, ", "),
			)
		}
		c.Database.InitAddr = net.JoinHostPort(initHost, "9042")

		// Create keyspace if needed
		ok, err := keyspaceExists(ctx, c, logger)
		if err != nil {
			return errors.Wrapf(err, "db init")
		}
		if !ok {
			logger.Info(ctx, "Creating keyspace", "keyspace", c.Database.Keyspace)
			if err := createKeyspace(ctx, c, logger); err != nil {
				return errors.Wrapf(err, "db init")
			}
			logger.Info(ctx, "Keyspace created", "keyspace", c.Database.Keyspace)
		}

		// Migrate schema
		logger.Info(ctx, "Migrating schema", "keyspace", c.Database.Keyspace)
		if err := migrateSchema(ctx, c, logger); err != nil {
			return errors.Wrapf(err, "db init")
		}
		logger.Info(ctx, "Schema up to date", "keyspace", c.Database.Keyspace)

		// Start server
		s, err := newServer(c, logger)
		if err != nil {
			return errors.Wrapf(err, "server init")
		}
		if err := s.makeServices(ctx); err != nil {
			return errors.Wrapf(err, "server init")
		}
		if err := s.makeServers(ctx); err != nil {
			return errors.Wrapf(err, "server init")
		}
		if err := s.startServices(ctx); err != nil {
			return errors.Wrapf(err, "server start")
		}
		s.startServers(ctx)
		defer func() {
			s.shutdownServers(ctx, 30*time.Second)
			s.close()
		}()

		select {
		case <-ctx.Done():
		case err := <-s.errCh:
			if err != nil {
				return err
			}
		}

		return nil
	},
}

func init() {
	f := rootCmd.Flags()
	f.StringSliceVarP(&rootArgs.configFiles, "config-file", "c", []string{"/etc/scylla-manager/scylla-manager.yaml"}, "configuration file `path`")
	f.BoolVar(&rootArgs.version, "version", false, "print version and exit")
}
