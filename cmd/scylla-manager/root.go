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
	"syscall"
	"text/template"
	"time"

	"github.com/gocql/gocql"
	"github.com/google/gops/agent"
	"github.com/pkg/errors"
	"github.com/scylladb/gocqlx"
	"github.com/scylladb/gocqlx/migrate"
	"github.com/scylladb/mermaid"
	"github.com/scylladb/mermaid/log"
	"github.com/scylladb/mermaid/log/gocqllog"
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
	Use:          "scylla-manager",
	Short:        "Scylla Manager server",
	Args:         cobra.NoArgs,
	SilenceUsage: true,

	RunE: func(cmd *cobra.Command, args []string) error {
		// print version and return
		if cfgVersion {
			fmt.Fprintf(cmd.OutOrStdout(), "%s\n", mermaid.Version())
			return nil
		}

		// in debug mode launch gops agent
		if cfgDeveloperMode {
			if err := agent.Listen(&agent.Options{NoShutdownCleanup: true}); err != nil {
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
			return errors.Wrapf(err, "configuration %q", cfgConfigFile)
		}
		if err := config.validate(); err != nil {
			return errors.Wrapf(err, "configuration %q", cfgConfigFile)
		}

		// get a base context
		ctx := log.WithTraceID(context.Background())

		// create logger
		logger, err := logger(config)
		if err != nil {
			return errors.Wrapf(err, "logger")
		}

		// set gocql logger
		gocql.Logger = gocqllog.New(ctx, logger.Named("gocql"))

		// wait for database
		for {
			if err := tryConnect(config); err != nil {
				logger.Info(ctx, "could not connect to database", "error", err)
				time.Sleep(5 * time.Second)
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
		if err := migrateSchema(config); err != nil {
			return errors.Wrapf(err, "database migration")
		}

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
		s.close()

		// bye
		logger.Info(ctx, "Server stopped")
		logger.Sync()

		return nil
	},
}

func logger(config *serverConfig) (log.Logger, error) {
	if cfgDeveloperMode {
		return log.NewDevelopment(), nil
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

func migrateSchema(config *serverConfig) error {
	c := gocqlConfig(config)
	c.Timeout = config.Database.MigrateTimeout
	c.MaxWaitSchemaAgreement = config.Database.MigrateMaxWaitSchemaAgreement

	session, err := c.CreateSession()
	if err != nil {
		return err
	}
	defer session.Close()

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
