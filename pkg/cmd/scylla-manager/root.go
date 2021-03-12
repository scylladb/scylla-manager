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

	"github.com/gocql/gocql"
	"github.com/pkg/errors"
	"github.com/scylladb/go-log"
	"github.com/scylladb/go-log/gocqllog"
	"github.com/scylladb/scylla-manager/pkg"
	"github.com/scylladb/scylla-manager/pkg/callhome"
	"github.com/scylladb/scylla-manager/pkg/config"
	"github.com/scylladb/scylla-manager/pkg/util/netwait"
	"github.com/spf13/cobra"
	"go.uber.org/zap"
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

	RunE: func(cmd *cobra.Command, args []string) (runError error) {
		// Print version and return
		if rootArgs.version {
			fmt.Fprintf(cmd.OutOrStdout(), "%s\n", pkg.Version())
			return
		}

		// Read configuration
		config, err := config.ParseServerConfigFiles(rootArgs.configFiles)
		if err != nil {
			runError = errors.Wrapf(err, "configuration %q", rootArgs.configFiles)
			fmt.Fprintf(cmd.OutOrStderr(), "%s\n", runError)
			return
		}
		if err := config.Validate(); err != nil {
			runError = errors.Wrapf(err, "configuration %q", rootArgs.configFiles)
			fmt.Fprintf(cmd.OutOrStderr(), "%s\n", runError)
			return
		}

		// Get a base context
		ctx := log.WithNewTraceID(context.Background())

		// Create logger
		logger, err := makeLogger(config.Logger)
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
		logger.Info(ctx, "Using config", "config", obfuscatePasswords(config), "config_files", rootArgs.configFiles)

		// Redirect standard logger to the logger
		zap.RedirectStdLog(log.BaseOf(logger))
		// Set logger to netwait
		netwait.DefaultWaiter.Logger = logger.Named("wait")

		// Set gocql logger
		gocql.Logger = gocqllog.StdLogger{
			BaseCtx: ctx,
			Logger:  logger.Named("gocql"),
		}

		// Wait for database
		logger.Info(ctx, "Checking database connectivity...")
		initHost, err := netwait.AnyHostPort(ctx, config.Database.Hosts, "9042")
		if err != nil {
			return errors.Wrapf(
				err,
				"no connection to database, make sure Scylla server is running and that database section in config file(s) %s is set correctly",
				strings.Join(rootArgs.configFiles, ", "),
			)
		}
		config.Database.InitAddr = net.JoinHostPort(initHost, "9042")

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
		server.startServers(ctx)
		defer func() {
			server.shutdownServers(ctx, 30*time.Second)
			server.close()
		}()

		// Wait signal
		signalCh := make(chan os.Signal, 1)
		signal.Notify(signalCh, syscall.SIGINT, syscall.SIGTERM)
		select {
		case err := <-server.errCh:
			if err != nil {
				return err
			}
		case sig := <-signalCh:
			logger.Info(ctx, "Received signal", "signal", sig)
		}

		return nil
	},
}

func makeLogger(c config.LogConfig) (log.Logger, error) {
	if c.Development {
		return log.NewDevelopmentWithLevel(c.Level), nil
	}

	return log.NewProduction(log.Config{
		Mode:  c.Mode,
		Level: zap.NewAtomicLevelAt(c.Level),
	})
}

func obfuscatePasswords(config *config.ServerConfig) config.ServerConfig {
	cfg := *config
	cfg.Database.Password = strings.Repeat("*", len(cfg.Database.Password))
	return cfg
}

func init() {
	f := rootCmd.Flags()
	f.StringSliceVarP(&rootArgs.configFiles, "config-file", "c",
		[]string{"/etc/scylla-manager/scylla-manager.yaml"},
		"repeatable argument to supply one or more configuration file `paths`")
	f.BoolVar(&rootArgs.version, "version", false, "print product version and exit")
}
