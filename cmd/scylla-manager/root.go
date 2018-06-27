// Copyright (C) 2017 ScyllaDB

package main

import (
	"bytes"
	"context"
	"fmt"
	"io/ioutil"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"text/template"
	"time"

	"github.com/gocql/gocql"
	"github.com/google/gops/agent"
	"github.com/pkg/errors"
	"github.com/scylladb/gocqlx"
	"github.com/scylladb/gocqlx/migrate"
	log "github.com/scylladb/golog"
	gocqllog "github.com/scylladb/golog/gocql"
	"github.com/scylladb/mermaid"
	"github.com/scylladb/mermaid/schema/cql"
	"github.com/spf13/cobra"
)

var (
	cfgConfigFile    string
	cfgDeveloperMode bool
	cfgVersion       bool
)

func init() {
	rootCmd.Flags().StringVarP(&cfgConfigFile, "config-file", "c", "/etc/scylla-manager/scylla-manager.yaml", "configuration file `path`")
	rootCmd.Flags().BoolVar(&cfgDeveloperMode, "developer-mode", false, "run in developer mode")
	rootCmd.Flags().BoolVar(&cfgVersion, "version", false, "print product version and exit")
}

var rootCmd = &cobra.Command{
	Use:           "scylla-manager",
	Short:         "Scylla Manager server",
	Args:          cobra.NoArgs,
	SilenceUsage:  true,
	SilenceErrors: true,

	RunE: func(cmd *cobra.Command, args []string) (runError error) {
		// print version and return
		if cfgVersion {
			fmt.Fprintf(cmd.OutOrStdout(), "%s\n", mermaid.Version())
			return
		}

		// in debug mode launch gops agent
		if cfgDeveloperMode {
			if err := agent.Listen(agent.Options{ShutdownCleanup: false}); err != nil {
				return errors.Wrapf(err, "gops agent startup")
			}
			defer agent.Close()
		}

		// try to make absolute path
		if absp, err := filepath.Abs(cfgConfigFile); err == nil {
			cfgConfigFile = absp
		}

		// read configuration
		config, err := newConfigFromFile(cfgConfigFile)
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

		// get a base context
		ctx := log.WithTraceID(context.Background())

		// create logger
		logger, err := logger(config)
		if err != nil {
			runError = errors.Wrapf(err, "logger")
			fmt.Fprintf(cmd.OutOrStderr(), "%s\n", runError)
			return
		}
		defer func() {
			if runError != nil {
				logger.Error(ctx, "Bye", "error", errors.Cause(runError))
			} else {
				logger.Info(ctx, "Bye")
			}
			logger.Sync() // nolint
		}()
		logger.Debug(ctx, "Using config", "config", obfuscatePasswords(config))

		// set gocql logger
		gocql.Logger = gocqllog.StdLogger{
			BaseCtx: ctx,
			Logger:  logger.Named("gocql"),
		}

		// wait for database
		for {
			if err := tryConnect(config); err != nil {
				const wait = 5 * time.Second
				logger.Info(ctx, "Could not connect to database",
					"sleep", wait,
					"error", err,
				)
				time.Sleep(wait)
			} else {
				break
			}
		}

		// create manager keyspace
		logger.Info(ctx, "Using keyspace",
			"keyspace", config.Database.Keyspace,
			"template", config.Database.KeyspaceTplFile,
		)
		if err := createKeyspace(config); err != nil {
			return errors.Wrapf(err, "database")
		}

		// migrate schema
		logger.Info(ctx, "Migrating schema", "dir", config.Database.MigrateDir)
		if err := migrateSchema(config, logger); err != nil {
			return errors.Wrapf(err, "database migration")
		}
		logger.Info(ctx, "Migrating schema done")

		// start server
		s, err := newServer(config, logger)
		if err != nil {
			return errors.Wrapf(err, "server init")
		}
		if err := s.startServices(ctx); err != nil {
			return errors.Wrapf(err, "server start")
		}
		s.startHTTPServers(ctx)
		defer s.close()

		logger.Info(ctx, "Service started")

		// wait signal
		signalCh := make(chan os.Signal, 1)
		signal.Notify(signalCh, syscall.SIGINT, syscall.SIGTERM)
		select {
		case err := <-s.errCh:
			if err != nil {
				logger.Error(ctx, "Server error", "error", err)
			}
		case sig := <-signalCh:
			{
				logger.Info(ctx, "Received signal", "signal", sig)
			}
		}

		// close
		s.shutdownServers(ctx, 30*time.Second)

		return
	},
}

func logger(config *serverConfig) (log.Logger, error) {
	if cfgDeveloperMode {
		return log.NewDevelopmentWithLevel(config.Logger.Level), nil
	}
	return log.NewProduction(config.Logger)
}

func tryConnect(config *serverConfig) error {
	c := gocqlConfig(config)
	c.Keyspace = "system"

	sesion, err := c.CreateSession()
	if sesion != nil {
		sesion.Close()
	}
	return err
}

func createKeyspace(config *serverConfig) error {
	stmt, err := readKeyspaceTplFile(config)
	if err != nil {
		return err
	}

	c := gocqlConfig(config)
	c.Keyspace = "system"
	c.Timeout = config.Database.MigrateTimeout
	c.MaxWaitSchemaAgreement = config.Database.MigrateMaxWaitSchemaAgreement

	session, err := c.CreateSession()
	if err != nil {
		return err
	}
	defer session.Close()

	return gocqlx.Query(session.Query(stmt), nil).ExecRelease()
}

func readKeyspaceTplFile(config *serverConfig) (stmt string, err error) {
	b, err := ioutil.ReadFile(config.Database.KeyspaceTplFile)
	if err != nil {
		return "", errors.Wrapf(err, "could not read file %s", config.Database.KeyspaceTplFile)
	}

	t := template.New("")
	if _, err := t.Parse(string(b)); err != nil {
		return "", errors.Wrapf(err, "template error file %s", config.Database.KeyspaceTplFile)
	}

	buf := new(bytes.Buffer)
	if err := t.Execute(buf, config.Database); err != nil {
		return "", errors.Wrapf(err, "template error file %s", config.Database.KeyspaceTplFile)
	}

	return buf.String(), err
}

func migrateSchema(config *serverConfig, logger log.Logger) error {
	c := gocqlConfig(config)
	c.Timeout = config.Database.MigrateTimeout
	c.MaxWaitSchemaAgreement = config.Database.MigrateMaxWaitSchemaAgreement

	session, err := c.CreateSession()
	if err != nil {
		return err
	}
	defer session.Close()

	cql.Logger = logger
	migrate.Callback = cql.MigrateCallback

	return migrate.Migrate(context.Background(), session, config.Database.MigrateDir)
}

func gocqlConfig(config *serverConfig) *gocql.ClusterConfig {
	c := gocql.NewCluster(config.Database.Hosts...)

	// consistency
	if config.Database.ReplicationFactor == 1 {
		c.Consistency = gocql.One
	} else {
		c.Consistency = gocql.LocalQuorum
	}
	c.Keyspace = config.Database.Keyspace
	c.Timeout = config.Database.Timeout

	// authentication
	if config.Database.User != "" {
		c.Authenticator = gocql.PasswordAuthenticator{
			Username: config.Database.User,
			Password: config.Database.Password,
		}
	}

	return c
}

func obfuscatePasswords(config *serverConfig) serverConfig {
	cfg := *config
	cfg.Database.Password = strings.Repeat("*", len(cfg.Database.Password))
	return cfg
}
