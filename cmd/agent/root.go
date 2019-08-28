// Copyright (C) 2017 ScyllaDB

package main

import (
	"context"
	"fmt"
	"io/ioutil"
	"net/http"

	"github.com/pkg/errors"
	"github.com/scylladb/go-log"
	"github.com/scylladb/mermaid"
	"github.com/scylladb/mermaid/internal/httputil/pprof"
	"github.com/scylladb/mermaid/rclone"
	"github.com/scylladb/mermaid/rclone/rcserver"
	"github.com/spf13/cobra"
	"go.uber.org/zap"
	"gopkg.in/yaml.v2"
)

var rootArgs = struct {
	configFile string
	version    bool
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
			fmt.Fprintf(cmd.OutOrStdout(), "%s\n", mermaid.Version())
			return nil
		}

		// Parse config
		b, err := ioutil.ReadFile(rootArgs.configFile)
		if err != nil {
			return errors.Wrapf(err, "failed to read config file %s", rootArgs.configFile)
		}
		var c config
		if err := yaml.Unmarshal(b, &c); err != nil {
			return errors.Wrapf(err, "failed to parse config file %s", rootArgs.configFile)
		}

		// Get a base context
		ctx := log.WithNewTraceID(context.Background())

		// Create logger
		logger, err := logger(&c)
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
		logger.Info(ctx, "Using config", "config", c)

		// Redirect standard logger to the logger
		zap.RedirectStdLog(log.BaseOf(logger))

		// Pin to CPU if possible
		cpus, err := pinToCPU(c.CPU)
		if err != nil {
			logger.Info(ctx, "Running on all CPUs", "error", err.Error())
		} else {
			logger.Info(ctx, "Pinned to CPUs", "cpus", cpus)
		}

		// Init rclone config options
		rclone.SetDefaultConfig()
		// Redirect rclone logger to the logger
		rclone.RedirectLogPrint(logger.Named("rclone"))

		// Start servers
		errCh := make(chan error, 2)

		logger.Info(ctx, "Starting HTTPS server", "address", c.HTTPS)
		go func() {
			server := http.Server{
				Addr:    c.HTTPS,
				Handler: newRouter(c, rcserver.New(), http.DefaultClient),
			}
			errCh <- errors.Wrap(server.ListenAndServeTLS(c.TLSCertFile, c.TLSKeyFile), "HTTPS server failed to start")
		}()
		if c.Debug != "" {
			logger.Info(ctx, "Starting debug server", "address", c.Debug)
			server := http.Server{
				Addr:    c.Debug,
				Handler: pprof.Handler(),
			}
			errCh <- errors.Wrap(server.ListenAndServe(), "debug server failed to start")
		}

		return <-errCh
	},
}

func logger(c *config) (log.Logger, error) {
	if c.Logger.Development {
		return log.NewDevelopmentWithLevel(c.Logger.Level), nil
	}

	return log.NewProduction(log.Config{
		Mode:  c.Logger.Mode,
		Level: c.Logger.Level,
	})
}

func init() {
	f := rootCmd.Flags()
	f.StringVarP(&rootArgs.configFile, "config-file", "c", "/etc/scylla-manager-agent/scylla-manager-agent.yaml", "configuration file `path`")
	f.BoolVar(&rootArgs.version, "version", false, "print product version and exit")
}
