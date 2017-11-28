// Copyright (C) 2017 ScyllaDB

package main

import (
	"bytes"
	"context"
	"fmt"
	"io/ioutil"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"sync"
	"syscall"
	"text/template"
	"time"

	"github.com/gocql/gocql"
	"github.com/google/gops/agent"
	"github.com/pkg/errors"
	"github.com/scylladb/gocqlx"
	"github.com/scylladb/gocqlx/migrate"
	"github.com/scylladb/mermaid"
	"github.com/scylladb/mermaid/cluster"
	"github.com/scylladb/mermaid/log"
	"github.com/scylladb/mermaid/log/gocqllog"
	"github.com/scylladb/mermaid/repair"
	"github.com/scylladb/mermaid/restapi"
	"github.com/scylladb/mermaid/scylla"
	"github.com/scylladb/mermaid/uuid"
	"github.com/spf13/cobra"
)

var (
	cfgConfigFile    string
	cfgDeveloperMode bool
	cfgVersion       bool
)

func init() {
	rootCmd.Flags().StringVarP(&cfgConfigFile, "config-file", "c", "/etc/scylla-mgmt/scylla-mgmt.yaml", "configuration file `path`")
	rootCmd.Flags().BoolVar(&cfgDeveloperMode, "developer-mode", false, "run in developer mode")
	rootCmd.Flags().BoolVar(&cfgVersion, "version", false, "print product version and exit")
}

var rootCmd = &cobra.Command{
	Use:          "scylla-mgmt",
	Short:        "Scylla management server",
	SilenceUsage: true,

	RunE: func(cmd *cobra.Command, args []string) error {
		// print version and return
		if cfgVersion {
			fmt.Fprintf(cmd.OutOrStdout(), "%s\n", mermaid.Version())
			return nil
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
		logger, err := logger()
		if err != nil {
			return errors.Wrapf(err, "logger error")
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

		// create management keyspace
		logger.Info(ctx, "Using keyspace",
			"keyspace", config.Database.Keyspace,
			"template", config.Database.KeyspaceTplFile,
		)
		if err := createKeyspace(config); err != nil {
			return errors.Wrapf(err, "database error")
		}

		// migrate schema
		logger.Info(ctx, "Migrating schema", "dir", config.Database.MigrateDir)
		if err := migrateSchema(config); err != nil {
			return errors.Wrapf(err, "database migration error")
		}

		// create database session
		session, err := gocqlConfig(config).CreateSession()
		if err != nil {
			return errors.Wrapf(err, "database error")
		}
		defer session.Close()

		// create cluster service
		clusterSvc, err := cluster.NewService(session, logger.Named("cluster"))
		if err != nil {
			return errors.Wrapf(err, "cluster service error")
		}

		// create scylla REST client provider
		provider := scylla.NewCachedProvider(func(ctx context.Context, clusterID uuid.UUID) (*scylla.Client, error) {
			c, err := clusterSvc.GetClusterByID(ctx, clusterID)
			if err != nil {
				return nil, err
			}

			client, err := scylla.NewClient(c.Hosts, logger.Named("client"))
			if err != nil {
				return nil, err
			}
			return scylla.WithConfig(client, scylla.Config{
				"murmur3_partitioner_ignore_msb_bits": float64(12),
				"shard_count":                         float64(c.ShardCount),
			}), nil
		})

		// create repair service
		repairSvc, err := repair.NewService(session, provider.Client, logger.Named("repair"))
		if err != nil {
			return errors.Wrapf(err, "repair service error")
		}
		if err := repairSvc.FixRunStatus(ctx); err != nil {
			return errors.Wrapf(err, "repair service error")
		}

		// create REST handler
		handler := restapi.New(&restapi.Services{
			Cluster: clusterSvc,
			Repair:  repairSvc,
		}, logger.Named("restapi"))

		// in debug mode launch gops agent
		if cfgDeveloperMode {
			if err := agent.Listen(&agent.Options{NoShutdownCleanup: true}); err != nil {
				return errors.Wrapf(err, "gops agent startup error")
			}
		}

		// observe cluster changes
		go func() {
			ch := clusterSvc.Changes()
			for {
				c, ok := <-ch
				if !ok {
					return
				}

				provider.Invalidate(c.ID)

				if c.Current != nil {
					if err := repairSvc.SyncUnits(ctx, c.ID); err != nil {
						logger.Error(ctx, "failed to sync units", "error", err)
					}
				}
			}
		}()

		// listen and serve
		var (
			httpServer  *http.Server
			httpsServer *http.Server
			errCh       = make(chan error, 2)
		)

		if config.HTTP != "" {
			httpServer = &http.Server{
				Addr:    config.HTTP,
				Handler: handler,
			}
			go func() {
				logger.Info(ctx, "Starting HTTP", "address", httpServer.Addr)
				errCh <- httpServer.ListenAndServe()
			}()
		}

		if config.HTTPS != "" {
			httpsServer = &http.Server{
				Addr:    config.HTTPS,
				Handler: handler,
			}
			go func() {
				logger.Info(ctx, "Starting HTTPS", "address", httpsServer.Addr)
				errCh <- httpsServer.ListenAndServeTLS(config.TLSCertFile, config.TLSKeyFile)
			}()
		}

		// wait
		signalCh := make(chan os.Signal, 1)
		signal.Notify(signalCh, syscall.SIGINT, syscall.SIGTERM)
		select {
		case err := <-errCh:
			if err != nil {
				logger.Error(ctx, "Server error", "error", err)
			}
		case sig := <-signalCh:
			{
				logger.Info(ctx, "Received signal", "signal", sig)
			}
		}

		// graceful shutdown
		var (
			timeoutCtx, cancelFunc = context.WithTimeout(ctx, 30*time.Second)
			wg                     sync.WaitGroup
		)
		defer cancelFunc()

		if httpServer != nil {
			logger.Info(ctx, "Closing HTTP...")

			wg.Add(1)
			go func() {
				defer wg.Done()
				if err := httpServer.Shutdown(timeoutCtx); err != nil {
					logger.Info(ctx, "Closing HTTP error", "error", err)
				}
				httpServer.Close()
			}()
		}
		if httpsServer != nil {
			logger.Info(ctx, "Closing HTTPS...")

			wg.Add(1)
			go func() {
				defer wg.Done()
				if err := httpsServer.Shutdown(timeoutCtx); err != nil {
					logger.Info(ctx, "Closing HTTPS error", "error", err)
				}
				httpsServer.Close()
			}()
		}
		wg.Wait()

		// close cluster
		clusterSvc.Close()

		// close repair
		repairSvc.Close(ctx)

		// close agent
		if cfgDeveloperMode {
			agent.Close()
		}

		// bye
		logger.Info(ctx, "Server stopped")
		logger.Sync()

		return nil
	},
}

func logger() (log.Logger, error) {
	if cfgDeveloperMode {
		return log.NewDevelopment(), nil
	}
	return log.NewProduction("scylla-mgmt")
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
