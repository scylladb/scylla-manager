// Copyright (C) 2017 ScyllaDB

package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/gocql/gocql"
	"github.com/pkg/errors"
	"github.com/scylladb/go-log"
	"github.com/scylladb/go-log/gocqllog"
	"github.com/scylladb/mermaid"
	"github.com/spf13/cobra"
	"go.uber.org/zap"
)

var (
	cfgConfigFile []string
	cfgVersion    bool
)

func init() {
	rootCmd.Flags().StringSliceVarP(&cfgConfigFile, "config-file", "c", []string{"/etc/scylla-manager/scylla-manager.yaml"}, "configuration file `path`")
	rootCmd.Flags().BoolVar(&cfgVersion, "version", false, "print product version and exit")
}

var rootCmd = &cobra.Command{
	Use:           "scylla-manager",
	Short:         "Scylla Manager server",
	Args:          cobra.NoArgs,
	SilenceUsage:  true,
	SilenceErrors: true,

	RunE: func(cmd *cobra.Command, args []string) (runError error) {
		// Print version and return
		if cfgVersion {
			fmt.Fprintf(cmd.OutOrStdout(), "%s\n", mermaid.Version())
			return
		}

		// Read configuration
		config, err := newConfigFromFile(cfgConfigFile...)
		if err != nil {
			runError = errors.Wrapf(err, "configuration %q", cfgConfigFile)
			fmt.Fprintf(cmd.OutOrStderr(), "%s\n", runError)
			return
		}
		if err := config.validate(); err != nil {
			runError = errors.Wrapf(err, "configuration %q", cfgConfigFile)
			fmt.Fprintf(cmd.OutOrStderr(), "%s\n", runError)
			return
		}

		// Populate global variables
		mermaid.PrometheusScrapeInterval = config.PrometheusScrapeInterval

		// Get a base context
		ctx := log.WithNewTraceID(context.Background())

		// Create logger
		logger, err := logger(config)
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
		logger.Info(ctx, "Using config", "config", obfuscatePasswords(config), "config_files", cfgConfigFile)

		// Redirect standard logger to the logger
		zap.RedirectStdLog(log.BaseOf(logger))

		// Set gocql logger
		gocql.Logger = gocqllog.StdLogger{
			BaseCtx: ctx,
			Logger:  logger.Named("gocql"),
		}

		// Wait for database
		if err := waitForDatabase(ctx, config, logger); err != nil {
			return errors.Wrapf(err, "db init")
		}

		// Create keyspace if needed
		ok, err := keyspaceExists(config)
		if err != nil {
			return errors.Wrapf(err, "db init")
		}
		if !ok {
			logger.Info(ctx, "Creating keyspace", "keyspace", config.Database.Keyspace)
			if err := createKeyspace(config); err != nil {
				return errors.Wrapf(err, "db init")
			}
			logger.Info(ctx, "Keyspace created", "keyspace", config.Database.Keyspace)
		}

		// Migrate schema
		logger.Info(ctx, "Migrating schema", "keyspace", config.Database.Keyspace, "dir", config.Database.MigrateDir)
		if err := migrateSchema(config, logger); err != nil {
			return errors.Wrapf(err, "db init")
		}
		logger.Info(ctx, "Schema up to date", "keyspace", config.Database.Keyspace)

		// Start server
		server, err := newServer(config, logger)
		if err != nil {
			return errors.Wrapf(err, "server init")
		}
		if err := server.startServices(ctx); err != nil {
			return errors.Wrapf(err, "server start")
		}
		server.startHTTPServers(ctx)
		defer server.close()

		logger.Info(ctx, "Service started")

		// Wait signal
		signalCh := make(chan os.Signal, 1)
		signal.Notify(signalCh, syscall.SIGINT, syscall.SIGTERM)
		select {
		case err := <-server.errCh:
			if err != nil {
				logger.Error(ctx, "Server error", "error", err)
			}
		case sig := <-signalCh:
			logger.Info(ctx, "Received signal", "signal", sig)
		}

		// Close
		server.shutdownServers(ctx, 30*time.Second)

		return
	},
}

func logger(config *serverConfig) (log.Logger, error) {
	if config.Logger.Development {
		return log.NewDevelopmentWithLevel(config.Logger.Level), nil
	}

	return log.NewProduction(log.Config{
		Mode:  config.Logger.Mode,
		Level: config.Logger.Level,
	})
}

func obfuscatePasswords(config *serverConfig) serverConfig {
	cfg := *config
	cfg.Database.Password = strings.Repeat("*", len(cfg.Database.Password))
	return cfg
}
