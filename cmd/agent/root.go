// Copyright (C) 2017 ScyllaDB

package main

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"strings"
	"time"

	"github.com/pkg/errors"
	"github.com/scylladb/go-log"
	"github.com/scylladb/mermaid"
	"github.com/scylladb/mermaid/internal/httputil/middleware"
	"github.com/scylladb/mermaid/internal/httputil/pprof"
	"github.com/scylladb/mermaid/rclone"
	"github.com/scylladb/mermaid/rclone/rcserver"
	"github.com/spf13/cobra"
	"go.uber.org/zap"
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
		c, err := parseConfig(rootArgs.configFile)
		if err != nil {
			return err
		}

		// Get a base context
		ctx := log.WithNewTraceID(context.Background())

		// Create logger
		logger, err := logger(c)
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

		// Redirect standard logger to the logger
		zap.RedirectStdLog(log.BaseOf(logger))

		// Log config
		logger.Info(ctx, "Using config", "config", obfuscateSecrets(c), "config_file", rootArgs.configFile)

		// Instruct users to set auth token
		if c.AuthToken == "" {
			ip, _, _ := net.SplitHostPort(c.HTTPS)
			logger.Info(ctx, "WARNING! Scylla data is exposed on IP "+ip+", "+
				"protect it by specifying auth_token in config file", "config_file", rootArgs.configFile,
			)
		}

		// Get CPU
		var cpu = c.CPU
		if cpu == noCPU {
			logger.Info(ctx, "Looking for a free CPU to run on")
			if c, err := findFreeCPU(); err != nil {
				logger.Info(ctx, "Could not get a free CPU", "error", err.Error())
			} else {
				cpu = c
			}
		}
		// Pin to CPU if possible
		if cpu != noCPU {
			if err := pinToCPU(cpu); err != nil {
				logger.Error(ctx, "Failed to pin to CPU", "cpu", cpu, "error", err)
			} else {
				logger.Info(ctx, "Pinned to CPU", "cpu", cpu)
			}
		}

		// Init rclone config options
		rclone.SetDefaultConfig()
		// Redirect rclone logger to the logger
		rclone.RedirectLogPrint(logger.Named("rclone"))
		// Register rclone providers
		if err := rcserver.RegisterInMemoryConf(); err != nil {
			return err
		}
		if err := rcserver.RegisterLocalDirProvider("data", "Jailed Scylla data", "/var/lib/scylla/data"); err != nil {
			return err
		}

		// Start servers
		errCh := make(chan error, 2)

		logger.Info(ctx, "Starting HTTPS server", "address", c.HTTPS)
		go func() {
			var h http.Handler
			h = newRouter(c, rcserver.New(), http.DefaultClient)
			h = middleware.ValidateAuthToken(h, c.AuthToken, time.Second)

			server := http.Server{
				Addr:    c.HTTPS,
				Handler: h,
			}
			errCh <- errors.Wrap(server.ListenAndServeTLS(c.TLSCertFile, c.TLSKeyFile), "HTTPS server start")
		}()

		if c.Debug != "" {
			go func() {
				logger.Info(ctx, "Starting debug server", "address", c.Debug)
				server := http.Server{
					Addr:    c.Debug,
					Handler: pprof.Handler(),
				}
				errCh <- errors.Wrap(server.ListenAndServe(), "debug server start")
			}()
		}

		return <-errCh
	},
}

func logger(c config) (log.Logger, error) {
	if c.Logger.Development {
		return log.NewDevelopmentWithLevel(c.Logger.Level), nil
	}

	return log.NewProduction(log.Config{
		Mode:  c.Logger.Mode,
		Level: c.Logger.Level,
	})
}

func obfuscateSecrets(c config) config {
	cfg := c
	cfg.AuthToken = strings.Repeat("*", len(cfg.AuthToken))
	return cfg
}

func init() {
	f := rootCmd.Flags()
	f.StringVarP(&rootArgs.configFile, "config-file", "c", "/etc/scylla-manager-agent/scylla-manager-agent.yaml", "configuration file `path`")
	f.BoolVar(&rootArgs.version, "version", false, "print product version and exit")
}
